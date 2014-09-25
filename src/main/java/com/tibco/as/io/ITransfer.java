package com.tibco.as.io;

public interface ITransfer {

	void open() throws Exception;

	void close() throws Exception;

	void stop() throws Exception;

}
