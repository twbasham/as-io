package com.tibco.as.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;

public abstract class AbstractDestinationTransfer implements Runnable {

	private Logger log = LogFactory.getLog(AbstractDestinationTransfer.class);
	private ExecutorService service;
	private Destination destination;
	private IInputStream inputStream;
	private IOutputStream outputStream;

	public AbstractDestinationTransfer(Destination destination) {
		this.destination = destination;
	}

	public IInputStream getInputStream() {
		return inputStream;
	}

	@Override
	public void run() {
		int workers = getWorkerCount(destination);
		inputStream = getInputStream(destination);
		outputStream = getOutputStream(destination);
		service = Executors.newFixedThreadPool(workers);
		for (int index = 0; index < workers; index++) {
			service.execute(new Worker(inputStream, outputStream));
		}
		service.shutdown();
		try {
			while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
				// do nothing
			}
		} catch (InterruptedException e) {
			log.log(Level.WARNING, "Transfer interrupted", e);
		}
	}

	protected abstract IInputStream getInputStream(
			Destination destination);

	protected abstract IOutputStream getOutputStream(
			Destination destination);

	protected abstract int getWorkerCount(Destination destination);

	public void stop() throws Exception {
		inputStream.close();
	}

	public boolean hasCompleted() {
		if (service == null) {
			return false;
		}
		return service.isTerminated();
	}

	public String getName() {
		return destination.getName();
	}

}