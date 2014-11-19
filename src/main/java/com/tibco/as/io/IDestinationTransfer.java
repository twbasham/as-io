package com.tibco.as.io;

public interface IDestinationTransfer extends Runnable {

	IDestination getDestination();

	TransferConfig getConfig();

	boolean isTerminated();

	Long getPosition();

	String getName();

	Long size();

	void stop() throws Exception;

}
