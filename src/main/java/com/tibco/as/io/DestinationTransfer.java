package com.tibco.as.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;

public class DestinationTransfer implements Runnable {

	private Logger log = LogFactory.getLog(DestinationTransfer.class);
	private ExecutorService service;
	private String name;
	private int workers;
	private IInputStream inputStream;
	private IOutputStream outputStream;

	public DestinationTransfer(String name, int workers, IInputStream in,
			IOutputStream out) {
		this.name = name;
		this.workers = workers;
		this.inputStream = in;
		this.outputStream = out;
		service = Executors.newFixedThreadPool(workers);
	}

	@Override
	public void run() {
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

	public void stop() throws Exception {
		inputStream.close();
	}

	public boolean hasCompleted() {
		if (service == null) {
			return false;
		}
		return service.isTerminated();
	}

	public Long size() {
		return inputStream.size();
	}

	public String getName() {
		return name;
	}

	public Long getPosition() {
		return inputStream.getPosition();
	}

	public IInputStream getInputStream() {
		return inputStream;
	}

}