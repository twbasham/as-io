package com.tibco.as.io;

public interface ICloseable {

	void open() throws Exception;
	
	void close() throws Exception;
	
	boolean isClosed();

}
