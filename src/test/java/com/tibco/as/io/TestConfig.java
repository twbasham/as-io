package com.tibco.as.io;

import com.tibco.as.convert.Field;

public class TestConfig extends DestinationConfig {

	private IInputStream inputStream;
	private IOutputStream outputStream;
	private int importBatchSize = 1;

	@Override
	public TestConfig clone() {
		TestConfig clone = new TestConfig();
		copyTo(clone);
		return clone;
	}

	public IInputStream getInputStream() {
		return inputStream;
	}

	public void setInputStream(IInputStream inputStream) {
		this.inputStream = inputStream;
	}

	public IOutputStream getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(IOutputStream outputStream) {
		this.outputStream = outputStream;
	}

	public int getImportBatchSize() {
		return importBatchSize;
	}

	public void setImportBatchSize(int importBatchSize) {
		this.importBatchSize = importBatchSize;
	}

	@Override
	public Field createField() {
		return new TestFieldConfig(this);
	}

}
