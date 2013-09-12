package org.infinispan.loaders.jpa.configuration;

import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jpa.JpaStore;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@ConfigurationFor(JpaStore.class)
@BuiltBy(JpaStoreConfigurationBuilder.class)
public class JpaStoreConfiguration extends AbstractStoreConfiguration {
	final private String persistenceUnitName;
	final private Class<?> entityClass;
	final private long batchSize;

	protected JpaStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties,
	      String persistenceUnitName, Class<?> entityClass, long batchSize) {
	   super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);

		this.persistenceUnitName = persistenceUnitName;
		this.entityClass = entityClass;
		this.batchSize = batchSize;
	}

	public String persistenceUnitName() {
		return persistenceUnitName;
	}

	public Class<?> entityClass() {
		return entityClass;
	}

	public long batchSize() {
	   return batchSize;
	}
}
