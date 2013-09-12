package org.infinispan.loaders.jpa.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.commons.configuration.Builder;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaStoreConfigurationBuilder
		extends AbstractStoreConfigurationBuilder<JpaStoreConfiguration, JpaStoreConfigurationBuilder> {

	private String persistenceUnitName;
	private Class<?> entityClass;
	private long batchSize = 100;

	public JpaStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
		super(builder);
	}

	public JpaStoreConfigurationBuilder persistenceUnitName(String persistenceUnitName) {
		this.persistenceUnitName = persistenceUnitName;
		return self();
	}

	public JpaStoreConfigurationBuilder entityClass(Class<?> entityClass) {
		this.entityClass = entityClass;
		return self();
	}

	public JpaStoreConfigurationBuilder batchSize(long batchSize) {
	   this.batchSize = batchSize;
	   return self();
	}

	@Override
	public void validate() {
		// how do you validate required attributes?
		super.validate();
	}

	@Override
	public JpaStoreConfiguration create() {
		return new JpaStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(), singletonStore.create(), preload, shared, properties,
            persistenceUnitName, entityClass, batchSize);
	}

	@Override
	public Builder<?> read(JpaStoreConfiguration template) {
		persistenceUnitName = template.persistenceUnitName();
		entityClass = template.entityClass();
		batchSize = template.batchSize();

		// AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

		return self();
	}

	@Override
	public JpaStoreConfigurationBuilder self() {
		return this;
	}

}
