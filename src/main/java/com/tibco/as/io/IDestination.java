package com.tibco.as.io;

public interface IDestination {

	void start() throws Exception;

	void awaitTermination() throws InterruptedException;

	void stop() throws Exception;

	String getName();

	boolean hasCompleted();

	Long getPosition();

	Long size();

}