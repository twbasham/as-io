package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestDestination extends Destination {

	private Collection<IOutputStreamListener> listeners = new ArrayList<IOutputStreamListener>();
	private Long sleep;
	private TestChannel channel;

	public TestDestination(TestChannel channel) {
		super(channel);
		this.channel = channel;
	}

	@Override
	public TestDestination clone() {
		TestDestination destination = new TestDestination(channel);
		copyTo(destination);
		return destination;
	}

	public void copyTo(TestDestination target) {
		if (target.sleep == null) {
			target.sleep = sleep;
		}
		target.listeners.addAll(listeners);
		super.copyTo(target);
	}

	@Override
	protected Field newField() {
		Field field = super.newField();
		field.setJavaType(String.class);
		return field;
	}

	@Override
	public ListInputStream getInputStream() throws Exception {
		return new ListInputStream(this);
	}

	@Override
	public ListOutputStream getOutputStream() {
		return new ListOutputStream(this);
	}

	public List<Object> getList() {
		return channel.getList(getSpace());
	}

	public void setSleep(Long sleep) {
		this.sleep = sleep;
	}

	public Long getSleep() {
		return sleep;
	}

	public void write(Object[] object) {
		getList().add(object);
		for (IOutputStreamListener listener : listeners) {
			listener.wrote(object);
		}
	}

	public void addListener(IOutputStreamListener listener) {
		listeners.add(listener);
	}

}
