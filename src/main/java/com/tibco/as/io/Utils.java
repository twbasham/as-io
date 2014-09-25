package com.tibco.as.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;

public class Utils {

	public static final char EXTENSION_SEPARATOR = '.';

	public static String getExtension(String filename) {
		int extensionPos = getExtensionPosition(filename);
		if (extensionPos == -1 || extensionPos == filename.length() - 1) {
			return "";
		}
		return filename.substring(extensionPos + 1);
	}

	private static int getExtensionPosition(String filename) {
		if (filename == null) {
			return -1;
		}
		return filename.lastIndexOf(EXTENSION_SEPARATOR);
	}

	public static String getBaseName(String filename) {
		int position = getExtensionPosition(filename);
		if (position == -1) {
			return filename;
		}
		return filename.substring(0, position);
	}

	public static void copy(InputStream inp, OutputStream out)
			throws IOException {
		byte[] buff = new byte[4096];
		int count;
		while ((count = inp.read(buff)) != -1) {
			if (count > 0) {
				out.write(buff, 0, count);
			}
		}
	}

	public static File copy(String resource, File dir) throws IOException {
		return copy(resource, dir, resource);
	}

	public static InputStream getResourceAsStream(String resource) {
		return ClassLoader.getSystemClassLoader().getResourceAsStream(resource);
	}

	public static File createTempDirectory() throws IOException {
		File dir = File.createTempFile(Utils.class.getName(),
				String.valueOf(System.currentTimeMillis()));
		if (!dir.delete()) {
			throw new IOException(MessageFormat.format(
					"Could not delete temp file: {0}", dir.getAbsolutePath()));
		}
		if (!dir.mkdir()) {
			throw new IOException(MessageFormat.format(
					"Could not create temp directory: {0}",
					dir.getAbsolutePath()));
		}
		return dir;
	}

	public static File copy(String resource, File dir, String filename)
			throws IOException {
		File file = new File(dir, filename);
		copyToFile(resource, file);
		return file;
	}

	public static void copyToFile(String resource, File destination)
			throws IOException {
		OutputStream out = new FileOutputStream(destination);
		try {
			InputStream in = getResourceAsStream(resource);
			if (in == null) {
				throw new FileNotFoundException(resource);
			}
			try {
				Utils.copy(in, out);
			} finally {
				in.close();
			}
		} finally {
			out.close();
		}
	}

}
