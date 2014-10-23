package com.tibco.as.io;

public class TestDestination extends AbstractDestination {

	private TestDestinationConfig config;

	public TestDestination(TestChannel channel, TestDestinationConfig config) {
		super(channel, config);
		this.config = config;
	}

	@Override
	protected IInputStream getImportInputStream() throws Exception {
		return config.getInputStream();
	}

	@Override
	protected IOutputStream getExportOutputStream() {
		return config.getOutputStream();
	}

	@Override
	protected Class<?> getComponentType() {
		return Object.class;
	}

}
