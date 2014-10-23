package com.tibco.as.io;

import java.util.List;
import java.util.Vector;

public class ListOutputStream implements IOutputStream {

	private List<Object> list = new Vector<Object>();
	private Long sleep;

	public List<Object> getList() {
		return list;
	}

	@Override
	public void open() throws Exception {
	}

	@Override
	public void close() throws Exception {
	}

	private void sleep() throws InterruptedException {
		if (sleep == null) {
			return;
		}
		Thread.sleep(sleep);
	}

	@Override
	public void write(Object element) throws InterruptedException {
		list.add(element);
		sleep();
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

}
