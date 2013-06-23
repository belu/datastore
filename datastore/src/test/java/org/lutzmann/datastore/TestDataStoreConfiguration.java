package org.lutzmann.datastore;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class TestDataStoreConfiguration extends DataStoreConfiguration {

	private static final String DATA_FILE_EXTENSION = ".dat";

	private static final File DEFAULT_DATA_STORE_LOCATION = new File("testdata");

	protected File getDataStoreLocation() throws IOException {
		if (!DEFAULT_DATA_STORE_LOCATION.exists()) {
			if (!DEFAULT_DATA_STORE_LOCATION.mkdirs()) {
				throw new IOException("Unable to create data store location.");
			}
		}
		return DEFAULT_DATA_STORE_LOCATION;
	}

	@Override
	public final File getCurrentStoreFile() throws IOException {
		return new File(getDataStoreLocation(), "current" + DATA_FILE_EXTENSION);
	}

	@Override
	public final List<File> getSortedStoreFilesForRecovery() throws IOException {
		final List<File> dataFiles = Arrays.asList(getDataStoreLocation()
				.listFiles(new FileFilter() {

					@Override
					public boolean accept(final File pathname) {
						return (pathname.isFile() && pathname.getName()
								.endsWith(DATA_FILE_EXTENSION));
					}
				}));

		Collections.sort(dataFiles);

		return dataFiles;
	}

	@Override
	public Checksum createChecksumGenerator() {
		return new Adler32();
	}
}
