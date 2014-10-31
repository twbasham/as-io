package com.tibco.as.io;

public class TestDestination extends Destination {

	private ListInputStream inputStream = new ListInputStream();
	private ListOutputStream outputStream = new ListOutputStream(this);

	public TestDestination(TestChannel channel) {
		super(channel);
	}

	@Override
	protected Field newField() {
		Field field = super.newField();
		field.setJavaType(String.class);
		return field;
	}

	@Override
	public ListInputStream getInputStream() {
		return inputStream;
	}

	@Override
	public ListOutputStream getOutputStream() {
		return outputStream;
	}

	@Override
	public String getName() {
		return getSpace();
	}

}
