package com.tibco.as.io;

import java.util.List;

public class ListInputStream<T> implements IInputStream<T> {

	private List<T> list;

	private int position = 0;

	private Long sleep;

	public ListInputStream(List<T> list) {
		this.list = list;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public T read() throws Exception {
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
	public long size() {
		return list.size();
	}

	@Override
	public long getPosition() {
		return position;
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
	public String getName() {
		return "list";
	}
	
	@Override
	public long getOpenTime() {
		return 0;
	}

}
