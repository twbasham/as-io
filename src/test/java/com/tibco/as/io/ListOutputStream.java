package com.tibco.as.io;

public class ListOutputStream extends AbstractOutputStream<Object[]> {

	private TestDestination destination;

	public ListOutputStream(TestDestination destination) {
		super(destination);
		this.destination = destination;
	}

	private void sleep() throws InterruptedException {
		Long sleep = destination.getSleep();
		if (sleep == null) {
			return;
		}
		Thread.sleep(sleep);
	}

	@Override
	protected void doWrite(Object[] object) throws Exception {
		destination.write(object);
		sleep();
	}

	@Override
	protected Object[] newObject(int length) {
		return new String[length];
	}

}
