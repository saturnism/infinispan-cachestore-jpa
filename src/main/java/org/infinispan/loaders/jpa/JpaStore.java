package org.infinispan.loaders.jpa;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.GeneratedValue;
import javax.persistence.PersistenceException;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.Tuple;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.InternalMetadataImpl;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.MarshalledEntryImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask;
import org.infinispan.persistence.spi.AdvancedCacheLoader.KeyFilter;
import org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaStore implements AdvancedLoadWriteStore {
   private InitializationContext ctx;
	private JpaStoreConfiguration configuration;
	private AdvancedCache<?, ?> cache;
	private EntityManagerFactory emf;
	private EntityManagerFactoryRegistry emfRegistry;
	
	@Override
	public void init(InitializationContext ctx) {
	   this.ctx = ctx;
		this.cache = ctx.getCache().getAdvancedCache();
		this.emfRegistry = this.cache.getComponentRegistry().getGlobalComponentRegistry().getComponent(EntityManagerFactoryRegistry.class);
		this.configuration = ctx.getConfiguration();
	}

	@Override
	public void start() {
		try {
			this.emf = this.emfRegistry.getEntityManagerFactory(configuration.persistenceUnitName());
		} catch (PersistenceException e) {
			throw new JpaCacheLoaderException("Persistence Unit [" + configuration.persistenceUnitName() + "] not found", e);
		}

		ManagedType<?> mt;

		try {
			mt = emf.getMetamodel()
				.entity(configuration.entityClass());
		} catch (IllegalArgumentException e) {
			throw new JpaCacheLoaderException("Entity class [" + configuration.entityClass() + " specified in configuration is not recognized by the EntityManagerFactory with Persistence Unit [" + configuration.persistenceUnitName() + "]", e);
		}

		if (!(mt instanceof IdentifiableType)) {
			throw new JpaCacheLoaderException(
					"Entity class must have one and only one identifier (@Id or @EmbeddedId)");
		}
		IdentifiableType<?> it = (IdentifiableType<?>) mt;
		if (!it.hasSingleIdAttribute()) {
			throw new JpaCacheLoaderException(
					"Entity class has more than one identifier.  It must have only one identifier.");
		}

		Type<?> idType = it.getIdType();
		Class<?> idJavaType = idType.getJavaType();

		if (idJavaType.isAnnotationPresent(GeneratedValue.class)) {
			throw new JpaCacheLoaderException(
					"Entity class has one identifier, but it must not have @GeneratedValue annotation");
		}

	}

	public EntityManagerFactory getEntityManagerFactory() {
		return emf;
	}

	@Override
	public void stop() {
		try {
		   this.emfRegistry.closeEntityManagerFactory(configuration.persistenceUnitName());
		} catch (Throwable t) {
			throw new CacheLoaderException(
					"Exceptions occurred while stopping store", t);
		}
	}
	
	protected boolean isValidKeyType(Object key) {
		return emf.getMetamodel().entity(configuration.entityClass()).getIdType().getJavaType().isAssignableFrom(key.getClass());
	}

	@Override
	public void clear() {
		EntityManager em = emf.createEntityManager();
		EntityTransaction txn = em.getTransaction();

		try {
			txn.begin();

			String name = em.getMetamodel().entity(configuration.entityClass())
					.getName();
			Query query = em.createQuery("DELETE FROM " + name);
			query.executeUpdate();

			txn.commit();
		} catch (Exception e) {
			if (txn != null && txn.isActive())
				txn.rollback();
			throw new CacheLoaderException("Exception caught in clear()", e);
		} finally {
			em.close();
		}

	}
	
	@Override
	public MarshalledEntry load(Object key) {
		if (!isValidKeyType(key)) {
			return null;
		}

		EntityManager em = emf.createEntityManager();
		try {
			Object o = em.find(configuration.entityClass(), key);
			if (o == null)
				return null;

			// Immortal entries has no metadata
			return new MarshalledEntryImpl(key, o, null, ctx.getMarshaller());
		} finally {
			em.close();
		}

	}

	protected boolean includeKey(Object key, Set<Object> keysToExclude) {
		return keysToExclude == null || !keysToExclude.contains(key);
	}

   @Override
   public boolean contains(Object key) {
      return load(key) != null;
   }

   @Override
   public void write(MarshalledEntry entry) {
      EntityManager em = emf.createEntityManager();

      Object o = entry.getValue();
      try {
         if (!configuration.entityClass().isAssignableFrom(o.getClass())) {
            throw new JpaCacheLoaderException(
                  "This cache is configured with JPA CacheStore to only store values of type " + configuration.entityClass());
         } else {
            EntityTransaction txn = em.getTransaction();
            Object id = emf.getPersistenceUnitUtil().getIdentifier(o);
            if (!entry.getKey().equals(id)) {
               throw new JpaCacheLoaderException(
                     "Entity id value must equal to key of cache entry: "
                           + "key = [" + entry.getKey() + "], id = ["
                           + id + "]");
            }
            try {
               txn.begin();

               em.merge(o);

               txn.commit();
            } catch (Exception e) {
               if (txn != null && txn.isActive())
                  txn.rollback();
               throw new CacheLoaderException(
                     "Exception caught in store()", e);
            }
         }
      } finally {
         em.close();
      }

   }

   @Override
   public boolean delete(Object key) {
      if (!isValidKeyType(key)) {
         return false;
      }

      EntityManager em = emf.createEntityManager();
      try {
         Object o = em.find(configuration.entityClass(), key);
         if (o == null) {
            return false;
         }

         EntityTransaction txn = em.getTransaction();
         try {
            txn.begin();
            em.remove(o);
            txn.commit();

            return true;
         } catch (Exception e) {
            if (txn != null && txn.isActive())
               txn.rollback();
            throw new CacheLoaderException(
                  "Exception caught in removeLockSafe()", e);
         }
      } finally {
         em.close();
      }
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue,
         boolean fetchMetadata) {
      int batchSize = 100;
      ExecutorCompletionService ecs = new ExecutorCompletionService(executor);
      int tasks = 0;
      final TaskContext taskContext = new TaskContextImpl();
      
      EntityManager em = emf.createEntityManager();

      try {
         CriteriaBuilder cb = em.getCriteriaBuilder();
         CriteriaQuery cq = cb.createQuery(configuration.entityClass());
         cq.select(cq.from(configuration.entityClass()));

         TypedQuery q = em.createQuery(cq);
         PersistenceUnitUtil util = emf.getPersistenceUnitUtil();
         List<Object> entries = new ArrayList<Object>(batchSize);
         
         for (Object value : q.getResultList()) {
            em.detach(value);
            entries.add(value);
            
            if (entries.size() == batchSize) {
               final List<Object> batch = entries;
               entries = new ArrayList<Object>(batchSize);
               submitProcessTask(task, filter,ecs, taskContext, batch, util);
               tasks++;
            }
            if (!entries.isEmpty()) {
               submitProcessTask(task, filter,ecs, taskContext, entries, util);
               tasks++;
            }
         }
         
         PersistenceUtil.waitForAllTasksToComplete(ecs, tasks);
      } finally {
         em.close();
      }
   }
   
   @SuppressWarnings("unchecked")
   private void submitProcessTask(final CacheLoaderTask cacheLoaderTask, final KeyFilter filter, ExecutorCompletionService ecs,
                                  final TaskContext taskContext, final List<Object> batch, final PersistenceUnitUtil util) {
      ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            for (Object value : batch) {
               if (taskContext.isStopped())
                  break;
               Object key = util.getIdentifier(value);
               if (filter == null || filter.shouldLoadKey(key))
                  cacheLoaderTask.processEntry(new MarshalledEntryImpl(key, value, null, ctx.getMarshaller()), taskContext);
            }
            return null;
         }
      });
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public int size() {
      EntityManager em = emf.createEntityManager();

      try {
         CriteriaBuilder cb = em.getCriteriaBuilder();
         CriteriaQuery cq = cb.createQuery(configuration.entityClass());
         cq.select(cq.from(configuration.entityClass()));

         TypedQuery q = em.createQuery(cq);

         return q.getResultList().size();
      } finally {
         em.close();
      }
   }

   @Override
   public void purge(Executor threadPool, PurgeListener listener) {
      // Immortal - no purging needed
   }
   
   public static InternalMetadata internalMetadata(InternalCacheEntry ice) {
      return ice.getMetadata() == null ? null : new InternalMetadataImpl(ice);
   }
}
