package com.tibco.as.io;

import java.util.concurrent.TimeUnit;

public interface ITransfer extends Runnable {

	void addListener(ITransferListener listener);

	void execute() throws TransferException;

	void stop() throws TransferException;

	void open() throws TransferException;

	void close() throws TransferException;

	long size();
	
	boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException;

	IInputStream<?> getInputStream();

}
