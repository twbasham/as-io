package com.tibco.as.io;

public class TestChannel extends AbstractChannel {

	@Override
	protected TestDestination newDestination() {
		return new TestDestination(this);
	}

}
