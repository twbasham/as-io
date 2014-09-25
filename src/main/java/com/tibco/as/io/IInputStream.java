package com.tibco.as.io;

public interface IInputStream<T> {

	public static final long UNKNOWN_SIZE = -1;

	void open() throws Exception;

	void close() throws Exception;

	boolean isClosed();

	/**
	 * @return next element in the stream
	 * @throws InterruptedException
	 *             if the stream was interrupted while reading
	 * @throws ReadException
	 *             if a problem occurs during read
	 */
	T read() throws Exception;

	long size();

	long getPosition();

	String getName();

	/**
	 * 
	 * @return time it took for input stream to open, in nanos
	 */
	long getOpenTime();

}