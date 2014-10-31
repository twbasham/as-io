package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

public class ListOutputStream extends AbstractOutputStream<Object> {

	private List<Object> list = new Vector<Object>();
	private Long sleep;
	private Collection<IOutputStreamListener> listeners = new ArrayList<IOutputStreamListener>();

	public ListOutputStream(Destination destination) {
		super(destination);
	}

	public List<Object> getList() {
		return list;
	}

	private void sleep() throws InterruptedException {
		if (sleep == null) {
			return;
		}
		Thread.sleep(sleep);
	}

	@Override
	protected void doWrite(Object[] object) throws Exception {
		list.add(object);
		sleep();
		for (IOutputStreamListener listener : listeners) {
			listener.wrote(object);
		}
	}

	public void addListener(IOutputStreamListener listener) {
		listeners.add(listener);
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	@Override
	protected Object[] newArray(int length) {
		return new String[length];
	}

}
