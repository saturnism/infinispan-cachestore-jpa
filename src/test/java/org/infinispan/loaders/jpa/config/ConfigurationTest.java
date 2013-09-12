package org.infinispan.loaders.jpa.config;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.loaders.jpa.JpaStore;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.loaders.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.loaders.jpa.entity.Document;
import org.infinispan.loaders.jpa.entity.User;
import org.infinispan.loaders.jpa.entity.Vehicle;
import org.infinispan.loaders.jpa.entity.VehicleId;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
@Test(groups = "unit", testName = "loaders.jpa.configuration.ConfigurationTest")
public class ConfigurationTest {

	public void testConfigBuilder() {
		GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
				.globalJmxStatistics().transport().defaultTransport().build();

		Configuration cacheConfig = new ConfigurationBuilder().persistence()
				.addStore(JpaStoreConfigurationBuilder.class)
				.persistenceUnitName("org.infinispan.loaders.jpa.configurationTest")
				.entityClass(User.class).build();
		
		
		StoreConfiguration cacheLoaderConfig = cacheConfig.persistence().stores().get(0);
		assert cacheLoaderConfig instanceof JpaStoreConfiguration;
		JpaStoreConfiguration jpaCacheLoaderConfig = (JpaStoreConfiguration) cacheLoaderConfig;
		assert jpaCacheLoaderConfig.persistenceUnitName().equals("org.infinispan.loaders.jpa.configurationTest");
		assert jpaCacheLoaderConfig.entityClass().equals(User.class);

		EmbeddedCacheManager cacheManager = new DefaultCacheManager(
				globalConfig);

		cacheManager.defineConfiguration("userCache", cacheConfig);

		cacheManager.start();
		Cache<String, User> userCache = cacheManager.getCache("userCache");
		User user = new User();
		user.setUsername("rtsang");
		user.setFirstName("Ray");
		user.setLastName("Tsang");
		userCache.put(user.getUsername(), user);
		userCache.stop();
		cacheManager.stop();
	}
	
	protected void validateConfig(Cache<VehicleId, Vehicle> vehicleCache) {
	   StoreConfiguration config = vehicleCache.getCacheConfiguration().persistence().stores().get(0);
	   
	   if (config instanceof JpaStoreConfiguration) {
	      JpaStoreConfiguration jpaConfig = (JpaStoreConfiguration) config;
	      assert jpaConfig.batchSize() == 1;
         assert jpaConfig.entityClass().equals(Vehicle.class) : jpaConfig.entityClass() + " != " + Vehicle.class;
         assert jpaConfig.persistenceUnitName().equals("org.infinispan.loaders.jpa.configurationTest") : jpaConfig.persistenceUnitName() + " != " + "org.infinispan.loaders.jpa.configurationTest";
	   } else {
	      assert false : "Unknown configuation class " + config.getClass();
	   }
	}
	
	public void testXmlConfig60() throws IOException {
		EmbeddedCacheManager cacheManager = new DefaultCacheManager(
				"config/jpa-config-60.xml");
		
		Cache<VehicleId, Vehicle> vehicleCache = cacheManager
				.getCache("vehicleCache");
		validateConfig(vehicleCache);
		
		Vehicle v = new Vehicle();
		v.setId(new VehicleId("NC", "123456"));
		v.setColor("BLUE");
		vehicleCache.put(v.getId(), v);

		vehicleCache.stop();
		cacheManager.stop();
	}
	
}
