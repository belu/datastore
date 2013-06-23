package org.lutzmann.datastore;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;

public abstract class DataStoreTest {

	protected DataStoreConfiguration dataStoreConfiguration = new TestDataStoreConfiguration();
	protected DataPersisterConfiguration dataPersisterConfiguration = new DataPersisterConfiguration(
			dataStoreConfiguration);

	protected DataPersister persister;
	protected DataRecoverer recoverer;

	@Before
	public void setUp() throws IOException {
		dataStoreConfiguration = new TestDataStoreConfiguration();
		dataPersisterConfiguration = new DataPersisterConfiguration(
				dataStoreConfiguration);

		deleteDataStoreFiles();

		persister = new DataPersister(dataPersisterConfiguration);
		recoverer = new DataRecoverer(dataStoreConfiguration);
	}

	protected void deleteDataStoreFiles() throws IOException {
		for (final File dataFile : dataStoreConfiguration
				.getSortedStoreFilesForRecovery()) {
			dataFile.delete();
		}
	}

	@After
	public void tearDown() throws Exception {
		persister.shutdown();
	}

	private final Random rand = new Random();

	protected byte[] createRandom(final int length) {
		final byte[] data = new byte[length];
		rand.nextBytes(data);
		return data;
	}
}
