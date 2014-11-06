package com.tibco.as.io;

public class ListInputStream implements IInputStream {

	private TestDestination destination;
	private Long position;
	private boolean open;

	public ListInputStream(TestDestination destination) {
		this.destination = destination;
	}

	@Override
	public synchronized void open() throws Exception {
		if (isClosed()) {
			position = 0L;
			open = true;
		}
	}

	@Override
	public synchronized Object read() throws Exception {
		if (isClosed()) {
			return null;
		}
		if (position < destination.getList().size()) {
			try {
				return destination.getList().get(position.intValue());
			} finally {
				if (destination.getSleep() != null) {
					Thread.sleep(destination.getSleep());
				}
				position++;
			}
		}
		return null;
	}

	@Override
	public Long size() {
		if (isClosed()) {
			return null;
		}
		return (long) destination.getList().size();
	}

	@Override
	public Long getPosition() {
		return position;
	}

	@Override
	public synchronized void close() throws Exception {
		open = false;
	}

	private boolean isClosed() {
		return !open;
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}
