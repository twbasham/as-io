package com.tibco.as.io;

public class TestDestination extends AbstractDestination {

	private TestConfig config;

	public TestDestination(TestChannel channel, TestConfig config) {
		super(channel, config);
		this.config = config;
	}

	@Override
	protected IInputStream getInputStream() throws Exception {
		return config.getInputStream();
	}

	@Override
	protected IOutputStream getOutputStream() {
		return config.getOutputStream();
	}

}
