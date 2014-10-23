package com.tibco.as.io;

import java.util.ArrayList;
import java.util.List;

public class ListInputStream implements IInputStream {

	private List<Object> list = new ArrayList<Object>();
	private int position = 0;
	private Long sleep;

	public List<Object> getList() {
		return list;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public Object read() throws Exception {
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
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	@Override
	public long getOpenTime() {
		return 0;
	}

}
