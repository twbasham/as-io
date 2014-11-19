package com.tibco.as.io.cli.converters;

import com.tibco.as.util.convert.Blob;

public class BlobConverter extends AbstractEnumConverter<Blob> {

	@Override
	protected Blob valueOf(String name) {
		return Blob.valueOf(name);
	}

	@Override
	protected Blob[] getValues() {
		return Blob.values();
	}

}
