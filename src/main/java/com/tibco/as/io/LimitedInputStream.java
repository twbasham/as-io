package com.tibco.as.io;

public class LimitedInputStream<T> implements IInputStream<T> {

	private IInputStream<T> in;
	private long limit;

	public LimitedInputStream(IInputStream<T> in, long limit) {
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
	public boolean isClosed() {
		return in.isClosed();
	}

	@Override
	public T read() throws Exception {
		if (getPosition() < limit) {
			return in.read();
		}
		return null;
	}

	@Override
	public long size() {
		return Math.min(in.size(), limit);
	}

	@Override
	public long getPosition() {
		return in.getPosition();
	}

	@Override
	public String getName() {
		return in.getName();
	}

	@Override
	public long getOpenTime() {
		return in.getOpenTime();
	}

}
