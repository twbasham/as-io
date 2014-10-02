package com.tibco.as.io;

import java.util.Arrays;
import java.util.Collection;

public class TestChannel extends AbstractChannel {

	protected TestChannel(ChannelConfig config) {
		super(config);
	}

	@Override
	protected IDestination createDestination(DestinationConfig config) {
		return new TestDestination(this, (TestConfig) config);
	}

	@Override
	protected Collection<DestinationConfig> getImportConfigs(
			DestinationConfig config) throws Exception {
		return Arrays.asList(config);
	}

}
