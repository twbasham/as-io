package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.util.log.LogFactory;

public abstract class AbstractDestinationTransfer implements
		IDestinationTransfer {

	private static final int DEFAULT_WORKER_COUNT = 1;

	private Logger log = LogFactory.getLog(AbstractDestinationTransfer.class);
	private IDestination destination;
	private IInputStream in;
	private IOutputStream out;
	private Collection<Worker> workers = new ArrayList<Worker>();
	private ExecutorService executor;

	protected AbstractDestinationTransfer(IDestination destination) {
		this.destination = destination;
	}

	@Override
	public IDestination getDestination() {
		return destination;
	}

	private int getWorkerCount() {
		Integer workerCount = getConfig().getWorkerCount();
		if (workerCount == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return workerCount;
	}

	@Override
	public Long getPosition() {
		if (in == null) {
			return null;
		}
		return in.getPosition();
	}

	@Override
	public Long size() {
		if (in == null) {
			return null;
		}
		return in.size();
	}

	@Override
	public void stop() throws Exception {
		out.close();
		in.close();
	}

	@Override
	public void run() {
		in = getInputStream(getInputStream());
		out = getOutputStream();
		for (int index = 0; index < getWorkerCount(); index++) {
			workers.add(new Worker(in, out));
		}
		executor = Executors.newFixedThreadPool(workers.size());
		for (Worker worker : workers) {
			executor.execute(worker);
		}
		executor.shutdown();
		try {
			while (!executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
				// do nothing
			}
		} catch (InterruptedException e) {
			log.log(Level.WARNING, "Transfer interrupted", e);
		}
	}

	private IInputStream getInputStream(IInputStream in) {
		Long limit = getConfig().getLimit();
		if (limit == null) {
			return in;
		}
		return new LimitInputStream(in, limit);
	}

	protected abstract IInputStream getInputStream();

	protected abstract IOutputStream getOutputStream();

	@Override
	public boolean isTerminated() {
		return executor != null && executor.isTerminated();
	}

	@Override
	public String getName() {
		return destination.getName();
	}

}