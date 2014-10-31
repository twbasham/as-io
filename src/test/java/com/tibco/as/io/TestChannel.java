package com.tibco.as.io;

public class TestChannel extends Channel {

	@Override
	protected TestDestination newDestination() {
		return new TestDestination(this);
	}

}
