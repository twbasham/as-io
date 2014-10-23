package com.tibco.as.io;

import com.tibco.as.convert.Field;

public class TestDestinationConfig extends DestinationConfig {

	private ListInputStream inputStream = new ListInputStream();
	private ListOutputStream outputStream = new ListOutputStream();
	private int importBatchSize = 1;

	@Override
	public TestDestinationConfig clone() {
		TestDestinationConfig clone = new TestDestinationConfig();
		copyTo(clone);
		return clone;
	}

	public ListInputStream getInputStream() {
		return inputStream;
	}

	public void setInputStream(ListInputStream inputStream) {
		this.inputStream = inputStream;
	}

	public ListOutputStream getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(ListOutputStream outputStream) {
		this.outputStream = outputStream;
	}

	public int getImportBatchSize() {
		return importBatchSize;
	}

	public void setImportBatchSize(int importBatchSize) {
		this.importBatchSize = importBatchSize;
	}

	@Override
	protected Field newField() {
		Field field = super.newField();
		field.setJavaType(String.class);
		return field;
	}

}
