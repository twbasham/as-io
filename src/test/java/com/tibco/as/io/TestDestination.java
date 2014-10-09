package com.tibco.as.io;

public class TestDestination extends AbstractDestination {

	private TestConfig config;

	public TestDestination(TestChannel channel, TestConfig config) {
		super(channel, config);
		this.config = config;
	}

	@Override
	protected IInputStream<Object[]> getInputStream() throws Exception {
		return config.getInputStream();
	}

	@Override
	protected IOutputStream<Object[]> getOutputStream() {
		return config.getOutputStream();
	}

	@Override
	protected String getExportName() {
		return "test";
	}

	@Override
	protected String getImportName() {
		return "test";
	}

}
