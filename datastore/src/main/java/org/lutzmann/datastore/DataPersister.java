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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.Checksum;

/**
 * This class is responsible to persist data to disk that later can be recovered
 * with the {@link DataRecoverer}.
 * 
 * @author belu
 * 
 */
public class DataPersister {

	private final Checksum checksum;
	private final ByteBuffer buffer;

	private final WritableByteChannel writeChannel;

	private final int maxDataLength;

	public DataPersister(final DataPersisterConfiguration configuration)
			throws IOException {
		this.checksum = configuration.getDataStoreConfiguration()
				.createChecksumGenerator();
		this.buffer = ByteBuffer.allocateDirect(configuration.getBufferSize());
		this.writeChannel = createAppendingChannel(configuration);
		this.maxDataLength = buffer.capacity() - HEADER_SIZE_BYTES;
	}

	private FileChannel createAppendingChannel(
			final DataPersisterConfiguration configuration) throws IOException {
		@SuppressWarnings("resource")
		final FileChannel fc = new RandomAccessFile(configuration
				.getDataStoreConfiguration().getCurrentStoreFile(),
				configuration.getMode()).getChannel();

		fc.position(fc.size());

		return fc;
	}

	/**
	 * Shuts down the data persister. The file handle will be closed. Remaining
	 * data in the underlying {@link ByteBuffer} is NOT flushed to disk.
	 * 
	 * After shutting down the {@link DataPersister} a call to
	 * {@link #writeToChannel()} will result in a {@link ClosedChannelException}
	 * .
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void shutdown() throws IOException {
		writeChannel.close();
	}

	/**
	 * Persists the given byte array to disk.
	 * 
	 * @param data
	 *            the byte array to persist
	 * @throws InsufficientCapacityException
	 *             if the underlying byte buffer has not enough capacity to hold
	 *             the given data
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void persist(final byte[] data) throws IOException {
		if (data.length > maxDataLength) {
			throw new InsufficientCapacityException();
		}

		writeToBuffer(data);

		writeToChannel();
	}

	public static final int HEADER_SIZE_BYTES = 12;

	private void writeToBuffer(final byte[] data) {
		buffer.clear();

		{ // Header
			buffer.putInt(data.length);
			buffer.putLong(calculateChecksum(data));
		}
		{ // Payload
			buffer.put(data);
		}

		buffer.flip();
	}

	private void writeToChannel() throws IOException {
		writeChannel.write(buffer);
	}

	private long calculateChecksum(final byte[] data) {
		checksum.reset();
		checksum.update(data, 0, data.length);
		return checksum.getValue();
	}
}
