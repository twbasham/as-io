package com.tibco.as.io;

import java.util.ArrayList;
import java.util.List;

public class ListOutputStream implements IOutputStream {

	private List<Object> list;

	private Long sleep;

	public ListOutputStream() {
		this(new ArrayList<Object>());
	}

	public ListOutputStream(List<Object> list) {
		this.list = list;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public void close() throws Exception {
		list = null;
	}

	private void sleep() throws InterruptedException {
		if (sleep != null) {
			Thread.sleep(sleep);
		}
	}

	@Override
	public void write(Object element) throws InterruptedException {
		list.add(element);
		sleep();
	}

	public boolean isClosed() {
		return list == null;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

}
