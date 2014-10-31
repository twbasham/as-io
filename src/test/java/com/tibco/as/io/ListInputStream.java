package com.tibco.as.io;

import java.util.List;
import java.util.Vector;

public class ListInputStream implements IInputStream {

	private List<Object> list = new Vector<Object>();
	private Long position;
	private Long sleep;
	private boolean open;

	public List<Object> getList() {
		return list;
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
		if (position < list.size()) {
			try {
				return list.get(position.intValue());
			} finally {
				if (sleep != null) {
					Thread.sleep(sleep);
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
		return (long) list.size();
	}

	@Override
	public Long getPosition() {
		return position;
	}

	@Override
	public synchronized void close() throws Exception {
		open = false;
	}

	@Override
	public boolean isClosed() {
		return !open;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}
