/*
 * Copyright 2013 Bernhard Lutzmann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lutzmann.datastore;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.Checksum;

/**
 * This class is responsible to recover data that has been persisted with the
 * {@link DataPersister}.
 * 
 * @author belu
 * 
 */
public final class DataRecoverer {

	private final DataStoreConfiguration configuration;
	private final Checksum checksum;

	public DataRecoverer(final DataStoreConfiguration configuration) {
		this.configuration = configuration;
		this.checksum = configuration.createChecksumGenerator();
	}

	public void recover(final DataProcessor processor) throws IOException,
			InvalidChecksumException {
		for (final File dataStoreFile : configuration
				.getSortedStoreFilesForRecovery()) {
			final DataInputStream is = new DataInputStream(new FileInputStream(
					dataStoreFile));
			try {
				recover(is, processor);
			} finally {
				is.close();
			}
		}
	}

	private void recover(final DataInputStream is, final DataProcessor processor)
			throws IOException, InvalidChecksumException {
		while (true) {
			final int length;
			try {
				length = is.readInt();
			} catch (final EOFException e) {
				break;
			}

			final long expectedChecksum = is.readLong();

			final byte[] data = new byte[length];

			int off = 0;
			int len = length;
			while (off != length) {
				final int bytesRead = is.read(data, off, len);

				off += bytesRead;
				len -= bytesRead;
			}

			final long actualChecksum = calculateChecksum(data);
			if (actualChecksum != expectedChecksum) {
				throw new InvalidChecksumException();
			}

			processor.onData(data);
		}
	}

	private long calculateChecksum(final byte[] data) {
		checksum.reset();
		checksum.update(data, 0, data.length);
		return checksum.getValue();
	}
}
