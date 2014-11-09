package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tibco.as.space.FieldDef;

public class TestDestination extends Destination {

	private Collection<IOutputStreamListener> listeners = new ArrayList<IOutputStreamListener>();
	private Long sleep;
	private TestChannel channel;

	public TestDestination(TestChannel channel) {
		super(channel);
		this.channel = channel;
	}

	@Override
	public void copyTo(Destination destination) {
		TestDestination target = (TestDestination) destination;
		if (target.sleep == null) {
			target.sleep = sleep;
		}
		target.listeners.addAll(listeners);
		super.copyTo(destination);
	}

	@Override
	public ListInputStream getInputStream() {
		return new ListInputStream(this);
	}

	@Override
	public ListOutputStream getOutputStream() {
		return new ListOutputStream(this);
	}

	public List<Object> getList() {
		return channel.getList(getSpaceName());
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

	@Override
	protected Class<String> getJavaType(FieldDef fieldDef) {
		return String.class;
	}

}
