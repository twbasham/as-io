package com.tibco.as.io;

public interface IDestinationTransfer extends Runnable {

	void prepare() throws Exception;

	Destination getDestination();

	boolean isRunning();

	Long getPosition();

	String getName();

	Long size();

	void stop() throws Exception;

}
