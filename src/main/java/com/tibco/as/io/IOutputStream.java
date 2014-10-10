package com.tibco.as.io;

public interface IOutputStream {

	void open() throws Exception;

	void close() throws Exception;

	void write(Object element) throws Exception;

}