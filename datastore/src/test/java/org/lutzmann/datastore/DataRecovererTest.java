package org.lutzmann.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.junit.Test;

public class DataRecovererTest extends DataStoreTest {

	@Test
	public void testRecoverer() throws Exception {
		final int START_SIZE = 8;
		final int END_SIZE = 1024;

		final int EXPECTED = END_SIZE - START_SIZE + 1;

		for (int i = START_SIZE; i <= END_SIZE; i++) {
			persister.persist(createRandom(i));
		}

		persister.shutdown();

		final AtomicLong counter = new AtomicLong(0);
		final AtomicInteger expectedSize = new AtomicInteger(START_SIZE);

		recoverer.recover(new DataProcessor() {

			@Override
			public void onData(final byte[] data) {
				assertEquals(expectedSize.getAndIncrement(), data.length);
				counter.incrementAndGet();
			}
		});

		assertEquals(EXPECTED, counter.get());
	}

	@Test
	public void testRecoveryOfEmptyStoreReturnsNothing() throws Exception {
		persister.shutdown();

		recoverer.recover(new DataProcessor() {

			@Override
			public void onData(final byte[] data) {
				fail();
			}
		});
	}

	@Test
	public void testWrongChecksumThrowsException() throws Exception {
		final long BAD_ENTRY = 99;

		final DataStoreConfiguration invalidChecksumDataStoreConfiguration = new TestDataStoreConfiguration() {
			@Override
			public Checksum createChecksumGenerator() {
				return new Checksum() {

					private final Checksum checksum = new Adler32();
					private int counter = 0;

					@Override
					public void update(final byte[] b, final int off,
							final int len) {
						checksum.update(b, off, len);
					}

					@Override
					public void update(final int b) {
						checksum.update(b);
					}

					@Override
					public void reset() {
						checksum.reset();
					}

					@Override
					public long getValue() {
						if (++counter == BAD_ENTRY) {
							return checksum.getValue() + 1;
						}
						return checksum.getValue();
					}
				};
			}
		};

		persister = new DataPersister(new DataPersisterConfiguration(
				invalidChecksumDataStoreConfiguration));

		for (int i = 0; i < 100; i++) {
			persister.persist(createRandom(256));
		}

		final AtomicLong counter = new AtomicLong(0);
		try {
			recoverer.recover(new DataProcessor() {

				@Override
				public void onData(final byte[] data) {
					counter.incrementAndGet();
				}
			});
			fail();
		} catch (final InvalidChecksumException e) {
			assertEquals(BAD_ENTRY - 1, counter.get());
		}
	}
}
