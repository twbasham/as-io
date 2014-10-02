package com.tibco.as.io;

public class TestConfig extends DestinationConfig {

	private IInputStream<String[]> inputStream;

	private IOutputStream<String[]> outputStream;

	private int importBatchSize = 1;

	public IInputStream<String[]> getInputStream() {
		return inputStream;
	}

	public void setInputStream(IInputStream<String[]> inputStream) {
		this.inputStream = inputStream;
	}

	public IOutputStream<String[]> getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(IOutputStream<String[]> outputStream) {
		this.outputStream = outputStream;
	}

	public int getImportBatchSize() {
		return importBatchSize;
	}

	public void setImportBatchSize(int importBatchSize) {
		this.importBatchSize = importBatchSize;
	}

}
