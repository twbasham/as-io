package com.tibco.as.io;

public class LimitedInputStream implements IInputStream {

	private IInputStream in;
	private long limit;

	public LimitedInputStream(IInputStream in, long limit) {
		this.in = in;
		this.limit = limit;
	}

	@Override
	public void open() throws Exception {
		in.open();
	}

	@Override
	public void close() throws Exception {
		in.close();
	}

	@Override
	public Object read() throws Exception {
		if (getPosition() < limit) {
			return in.read();
		}
		return null;
	}

	@Override
	public Long size() {
		if (in.size() == null) {
			return limit;
		}
		return Math.min(in.size(), limit);
	}

	@Override
	public Long getPosition() {
		return in.getPosition();
	}

	@Override
	public long getOpenTime() {
		return in.getOpenTime();
	}

	@Override
	public boolean isOpen() {
		return in.isOpen();
	}

}
