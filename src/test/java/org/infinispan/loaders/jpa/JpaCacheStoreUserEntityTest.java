package org.infinispan.loaders.jpa;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.loaders.jpa.entity.User;
import org.testng.annotations.Test;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "loaders.jpa.JpaCacheStoreUserEntityTest")
public class JpaCacheStoreUserEntityTest extends BaseJpaCacheStoreTest {
   @Override
   protected JpaStoreConfiguration createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      
      JpaStoreConfigurationBuilder cfg = new JpaStoreConfigurationBuilder(lcb);
      
      cfg.persistenceUnitName("org.infinispan.loaders.jpa");
      cfg.entityClass(User.class);
      
      return cfg.create();
   }

   @Override
   protected TestObject createTestObject(String key, String value) {
      User user = new User();
      user.setUsername(key);
      user.setFirstName("firstName-" + value);
      user.setLastName("lastName-" + value);
      user.setNote("note-" + value);
      return new TestObject(user.getUsername(), user);
   }
}
