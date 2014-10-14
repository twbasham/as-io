package com.tibco.as.io;

import java.util.List;

public class ListInputStream implements IInputStream {

	private List<?> list;

	private int position = 0;

	private Long sleep;

	public ListInputStream(List<?> list) {
		this.list = list;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public Object read() throws Exception {
		if (list == null) {
			return null;
		}
		if (position < list.size()) {
			try {
				return list.get(position++);
			} finally {
				if (sleep != null) {
					Thread.sleep(sleep);
				}
			}
		}
		return null;
	}

	@Override
	public Long size() {
		return (long) list.size();
	}

	@Override
	public Long getPosition() {
		return (long) position;
	}

	@Override
	public void close() throws Exception {
		list = null;
	}

	@Override
	public boolean isClosed() {
		return list == null;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}
