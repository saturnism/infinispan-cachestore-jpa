package org.infinispan.loaders.jpa;

import static java.util.Collections.emptySet;
import static org.infinispan.test.TestingUtil.allEntries;
import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.persistence.BaseCacheStoreTest.Pojo;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.CollectionKeyFilter;
import org.infinispan.persistence.DummyLoaderContext;
import org.infinispan.persistence.MarshalledEntryImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.TransactionFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseCacheStoreFunctionalTest.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@SuppressWarnings("unchecked")
// this needs to be here for the test to run in an IDE
@Test(groups = "unit", testName = "loaders.BaseJpaCacheStoretest")
public abstract class BaseJpaCacheStoreTest extends AbstractInfinispanTest {
   protected JpaStore fcs;
   protected AdvancedLoadWriteStore cl;
   protected StoreConfiguration csc;

   protected TransactionFactory gtf = new TransactionFactory();
   
   private EmbeddedCacheManager cacheManager;

   protected BaseJpaCacheStoreTest() {
      gtf.init(false, false, true, false);
   }

   @BeforeMethod
   public void setUp() throws Exception {
      try {
         cacheManager = TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false);
         cl = createStore();
      } catch (Exception e) {
         //in IDEs this won't be printed which makes debugging harder
         e.printStackTrace();
         throw e;
      }
   }
   
   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      try {
         if (cl != null) {
            cl.clear();
            cl.stop();
         }
         TestingUtil.killCacheManagers(cacheManager);
      } finally {
         cl = null;
      }
   }

   
   protected StreamingMarshaller getMarshaller() {
      return cacheManager.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }
   
   public void testLoadAndStoreImmortal() throws CacheLoaderException {
      assertFalse(cl.contains("k"));
      MarshalledEntry k = createTestMarshalledEntry("k", "v");
      cl.write(k);

      assert cl.load(k.getKey()).getValue().equals(k.getValue());
      assert cl.load(k.getKey()).getMetadata() == null || cl.load(k.getKey()).getMetadata().expiryTime() == -1;
      assert cl.load(k.getKey()).getMetadata() == null || cl.load(k.getKey()).getMetadata().maxIdle() == -1;
      assert cl.contains(k.getKey());

      boolean removed = cl.delete("k2");
      assertFalse(removed);
   }

   private void assertIsEmpty() {
      assert TestingUtil.allEntries(cl).isEmpty();
   }
   
   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {
      MarshalledEntry k1 = createTestMarshalledEntry("k1", "v1");
      MarshalledEntry k2 = createTestMarshalledEntry("k2", "v2");
      
      assertFalse(cl.contains(k1.getKey()));
      assertFalse(cl.contains(k2.getKey()));
      
      cl.write(k1);
      cl.write(k2);
      
      sleepForStopStartTest();

      cl.stop();
      cl.start();
      
      assertEquals(cl.load(k1.getKey()).getValue(), k1.getValue());
      assertEquals(cl.load(k2.getKey()).getValue(), k2.getValue());
   }

   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(100);
   }

   public void testPreload() throws Exception {
      MarshalledEntry k1 = createTestMarshalledEntry("k1", "v1");
      MarshalledEntry k2 = createTestMarshalledEntry("k2", "v2");
      MarshalledEntry k3 = createTestMarshalledEntry("k3", "v3");
      
      cl.write(k1);
      cl.write(k2);
      cl.write(k3);

      Set<MarshalledEntry> set = TestingUtil.allEntries(cl);

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add(k1.getKey());
      expected.add(k2.getKey());
      expected.add(k3.getKey());
      for (MarshalledEntry se : set)
         assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testStoreAndRemove() throws CacheLoaderException {
      MarshalledEntry k1 = createTestMarshalledEntry("k1", "v1");
      MarshalledEntry k2 = createTestMarshalledEntry("k2", "v2");
      MarshalledEntry k3 = createTestMarshalledEntry("k3", "v3");
      MarshalledEntry k4 = createTestMarshalledEntry("k4", "v4");
      
      cl.write(k1);
      cl.write(k2);
      cl.write(k3);
      cl.write(k4);


      Set<MarshalledEntry> set = TestingUtil.allEntries(cl);

      assert set.size() == 4;
      Set expected = new HashSet();
      expected.add(k1.getKey());
      expected.add(k2.getKey());
      expected.add(k3.getKey());
      expected.add(k4.getKey());
      for (MarshalledEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();

      cl.delete(k1.getKey());
      cl.delete(k2.getKey());
      cl.delete(k3.getKey());

      set = TestingUtil.allEntries(cl);
      assert set.size() == 1;
      set.remove(k4.getKey());
      assert expected.isEmpty();
   }

   public void testLoadAll() throws CacheLoaderException {
      MarshalledEntry k1 = createTestMarshalledEntry("k1", "v1");
      MarshalledEntry k2 = createTestMarshalledEntry("k2", "v2");
      MarshalledEntry k3 = createTestMarshalledEntry("k3", "v3");
      MarshalledEntry k4 = createTestMarshalledEntry("k4", "v4");
      MarshalledEntry k5 = createTestMarshalledEntry("k5", "v4");

      cl.write(k1);
      cl.write(k2);
      cl.write(k3);
      cl.write(k4);
      cl.write(k5);

      Set<MarshalledEntry> s = TestingUtil.allEntries(cl);
      assert s.size() == 5 : "Expected 5 keys, was " + s;

      s = allEntries(cl, new CollectionKeyFilter(emptySet()));
      assert s.size() == 5 : "Expected 5 keys, was " + s;

      s = allEntries(cl, new CollectionKeyFilter(Collections.<Object>singleton(k3.getKey())));
      assert s.size() == 4 : "Expected 4 keys but was " + s;

      for (MarshalledEntry me: s)
         assertFalse(me.getKey().equals(k3.getKey()));
   }

   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      MarshalledValue key = new MarshalledValue(new Pojo().role("key"), true, getMarshaller());
      MarshalledValue key2 = new MarshalledValue(new Pojo().role("key2"), true, getMarshaller());
      MarshalledValue value = new MarshalledValue(new Pojo().role("value"), true, getMarshaller());

      assertFalse(cl.contains(key));
      cl.write(new MarshalledEntryImpl(key, value, null, getMarshaller()));

      assert cl.load(key).getValue().equals(value);
      assert cl.load(key).getMetadata() == null || cl.load(key).getMetadata().expiryTime() == - 1;
      assert cl.load(key).getMetadata() == null || cl.load(key).getMetadata().lifespan() == - 1;
      assert cl.contains(key);

      boolean removed = cl.delete(key2);
      assertFalse(removed);

      assert cl.delete(key);
   }

   /**
    * @return a mock cache for use with the cache store impls
    */
   protected Cache getCache() {
      String name = "mockCache-" + getClass().getName();
      return mockCache(name);
   }

   public static Cache mockCache(String name) {
      AdvancedCache cache = mock(AdvancedCache.class);
      ComponentRegistry registry = mock(ComponentRegistry.class);
      org.infinispan.configuration.cache.Configuration config =
            new ConfigurationBuilder()
                  .dataContainer()
                     .keyEquivalence(AnyEquivalence.getInstance())
                     .valueEquivalence(AnyEquivalence.getInstance())
               .build();

      when(cache.getName()).thenReturn(name);
      when(cache.getAdvancedCache()).thenReturn(cache);
      when(cache.getComponentRegistry()).thenReturn(registry);
      when(registry.getTimeService()).thenReturn(TIME_SERVICE);
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      when(cache.getCacheConfiguration()).thenReturn(config);
      return cache;
   }
   
   protected MarshalledEntry createTestMarshalledEntry(String key, String value) {
      TestObject object = createTestObject(key, value);
      return new MarshalledEntryImpl(object.getKey(), object.getValue(), null, getMarshaller());
   }
   
   protected JpaStore createStore() throws Exception {
      fcs = new JpaStore();
      ConfigurationBuilder cb = new ConfigurationBuilder();
      JpaStoreConfiguration cfg = createCacheStoreConfig(cb.persistence());
      fcs.init(new DummyLoaderContext(cfg, cacheManager.getCache(), getMarshaller()));
      fcs.start();
      return fcs;
   }
   
   abstract protected JpaStoreConfiguration createCacheStoreConfig(PersistenceConfigurationBuilder persistence);
   abstract protected TestObject createTestObject(String key, String value);
}
