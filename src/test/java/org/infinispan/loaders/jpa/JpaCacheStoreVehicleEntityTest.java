package org.infinispan.loaders.jpa;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.loaders.jpa.entity.User;
import org.infinispan.loaders.jpa.entity.Vehicle;
import org.infinispan.loaders.jpa.entity.VehicleId;
import org.testng.annotations.Test;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "loaders.jpa.JpaCacheStoreVehicleEntityTest")
public class JpaCacheStoreVehicleEntityTest extends BaseJpaCacheStoreTest {
   @Override
   protected JpaStoreConfiguration createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      
      JpaStoreConfigurationBuilder cfg = new JpaStoreConfigurationBuilder(lcb);
      
      cfg.persistenceUnitName("org.infinispan.loaders.jpa");
      cfg.entityClass(Vehicle.class);
      
      return cfg.create();
   }

   @Override
   protected TestObject createTestObject(String key, String value) {
      Vehicle v = new Vehicle();
      v.setId(new VehicleId("State-" + key, key));
      v.setColor(value);
      return new TestObject(v.getId(), v);
   }
}
