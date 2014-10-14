package com.tibco.as.io;

public class TestChannel extends AbstractChannel {

	protected TestChannel(ChannelConfig config) {
		super(config);
	}

	@Override
	protected IDestination createDestination(DestinationConfig config) {
		return new TestDestination(this, (TestConfig) config);
	}

}
