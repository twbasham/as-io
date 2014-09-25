package com.tibco.as.io;

import com.tibco.as.space.Metaspace;

public class TestChannel extends AbstractChannel {

	protected TestChannel(Metaspace metaspace) {
		super(metaspace);
	}

	@Override
	protected IDestination getDestination(DestinationConfig config)
			throws Exception {
		return new TestDestination(this, (TestConfig) config);
	}

}
