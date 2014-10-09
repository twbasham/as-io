package com.tibco.as.io;

public class TestConfig extends DestinationConfig {

	private IInputStream<Object[]> inputStream;

	private IOutputStream<Object[]> outputStream;

	private int importBatchSize = 1;

	public IInputStream<Object[]> getInputStream() {
		return inputStream;
	}

	public void setInputStream(IInputStream<Object[]> inputStream) {
		this.inputStream = inputStream;
	}

	public IOutputStream<Object[]> getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(IOutputStream<Object[]> outputStream) {
		this.outputStream = outputStream;
	}

	public int getImportBatchSize() {
		return importBatchSize;
	}

	public void setImportBatchSize(int importBatchSize) {
		this.importBatchSize = importBatchSize;
	}

	@Override
	public FieldConfig createFieldConfig() {
		return new TestFieldConfig();
	}

}
