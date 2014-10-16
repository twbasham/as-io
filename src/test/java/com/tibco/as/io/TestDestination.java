package com.tibco.as.io;

public class TestDestination extends AbstractDestination {

	private TestDestinationConfig config;

	public TestDestination(TestChannel channel, TestDestinationConfig config) {
		super(channel, config);
		this.config = config;
	}

	@Override
	protected IInputStream createInputStream() throws Exception {
		return config.getInputStream();
	}

	@Override
	protected IOutputStream createOutputStream() {
		return config.getOutputStream();
	}

}
