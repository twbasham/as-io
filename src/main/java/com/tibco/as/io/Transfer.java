package com.tibco.as.io;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Transfer implements ITransfer {

	private String name;
	private IInputStream in;
	private Collection<Worker> workers;
	private ExecutorService service;

	public Transfer(String name, IInputStream in, Collection<Worker> workers) {
		this.name = name;
		this.in = in;
		this.workers = workers;
	}

	@Override
	public long size() {
		return in.size();
	}

	@Override
	public long getPosition() {
		return in.getPosition();
	}

	@Override
	public void open() throws Exception {
		in.open();
		service = Executors.newFixedThreadPool(workers.size());
		for (Worker worker : workers) {
			service.execute(worker);
		}
	}

	@Override
	public void close() throws Exception {
		service.shutdown();
		try {
			while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
				// do nothing
			}
		} catch (InterruptedException e) {
			throw new Exception("Could not finish transfers", e);
		}
		in.close();
	}

	@Override
	public void stop() throws Exception {
		in.close();
	}

	@Override
	public String getName() {
		return name;
	}
}
