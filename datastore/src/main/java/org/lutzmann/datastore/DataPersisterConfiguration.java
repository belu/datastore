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

import java.io.RandomAccessFile;

public class DataPersisterConfiguration {

	private static final int DEFAULT_BUFFER_SIZE = 65536;

	private static final String DEFAULT_MODE = "rws";

	private final DataStoreConfiguration dataStoreConfiguration;

	public DataPersisterConfiguration(
			final DataStoreConfiguration dataStoreConfiguration) {
		this.dataStoreConfiguration = dataStoreConfiguration;
	}

	/**
	 * @return a mode as described in
	 *         {@link RandomAccessFile#RandomAccessFile(String, String)}
	 */
	public String getMode() {
		return DEFAULT_MODE;
	}

	public int getBufferSize() {
		return DEFAULT_BUFFER_SIZE;
	}

	public DataStoreConfiguration getDataStoreConfiguration() {
		return dataStoreConfiguration;
	}
}
