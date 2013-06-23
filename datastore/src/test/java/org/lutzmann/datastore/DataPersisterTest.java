package org.lutzmann.datastore;

import static org.junit.Assert.fail;

import java.nio.channels.ClosedChannelException;

import org.junit.Test;

public class DataPersisterTest extends DataStoreTest {

	@Test
	public void testPersist() throws Exception {
		for (int i = 0; i < 100; i++) {
			persister.persist(createRandom(256));
		}
	}

	@Test
	public void testPersistTooMuchDataThrowsException() throws Exception {
		final byte[] maxData = createRandom(dataPersisterConfiguration
				.getBufferSize() - DataPersister.HEADER_SIZE_BYTES);
		persister.persist(maxData);

		final byte[] tooMuchData = createRandom(maxData.length + 1);
		try {
			persister.persist(tooMuchData);
			fail();
		} catch (final InsufficientCapacityException e) {
			// expected
		}
	}

	@Test
	public void testPersistWithNullThrowsNullPointerException()
			throws Exception {
		try {
			persister.persist(null);
			fail();
		} catch (final NullPointerException e) {
			// expected
		}
	}

	@Test
	public void testPersistNothing() throws Exception {
		persister.persist(new byte[0]);
	}

	@Test
	public void testPersistAfterShutdownThrowsException() throws Exception {
		persister.shutdown();

		try {
			persister.persist(createRandom(1024));
			fail();
		} catch (final ClosedChannelException e) {
			// expected
		}
	}

	@Test
	public void testShutdownTwiceDoesNothing() throws Exception {
		persister.shutdown();
		persister.shutdown();
	}
}
