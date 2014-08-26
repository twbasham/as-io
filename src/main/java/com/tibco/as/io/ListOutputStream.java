package com.tibco.as.io;

import java.util.List;

public class ListOutputStream<T> implements IOutputStream<T> {

	private List<T> list;

	private Long sleep;

	public ListOutputStream(List<T> list) {
		this.list = list;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public void close() throws Exception {
		list = null;
	}

	@Override
	public void write(List<T> elements) throws InterruptedException {
		list.addAll(elements);
		sleep();
	}

	private void sleep() throws InterruptedException {
		if (sleep != null) {
			Thread.sleep(sleep);
		}
	}

	@Override
	public void write(T element) throws InterruptedException {
		list.add(element);
		sleep();
	}

	@Override
	public boolean isClosed() {
		return list == null;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

}
