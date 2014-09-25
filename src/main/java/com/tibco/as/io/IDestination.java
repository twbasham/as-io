package com.tibco.as.io;

public interface IDestination {

	void open() throws Exception;

	void close() throws Exception;

	void stop() throws Exception;
	
}