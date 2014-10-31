package com.tibco.as.io;

public interface IInputStream {

	void open() throws Exception;

	void close() throws Exception;

	boolean isOpen();

	/**
	 * @return next element in the stream
	 * @throws InterruptedException
	 *             if the stream was interrupted while reading
	 * @throws ReadException
	 *             if a problem occurs during read
	 */
	Object read() throws Exception;

	Long size();

	Long getPosition();

	/**
	 * 
	 * @return time it took for input stream to open, in nanos
	 */
	long getOpenTime();

}