package com.tibco.as.io;

public class TestChannelConfig extends ChannelConfig {

	@Override
	protected DestinationConfig newDestinationConfig() {
		return new TestDestinationConfig();
	}

}
