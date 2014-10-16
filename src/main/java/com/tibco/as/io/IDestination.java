package com.tibco.as.io;

public interface IDestination {

	void start() throws Exception;

	void stop() throws Exception;

	boolean isClosed();

	IInputStream getInputStream();

	String getName();

}