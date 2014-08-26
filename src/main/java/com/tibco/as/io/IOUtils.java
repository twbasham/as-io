package com.tibco.as.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOUtils {

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
	
	public static void copy(InputStream inp, OutputStream out) throws IOException {
		byte[] buff = new byte[4096];
		int count;
		while ((count = inp.read(buff)) != -1) {
			if (count > 0) {
				out.write(buff, 0, count);
			}
		}
	}
}
