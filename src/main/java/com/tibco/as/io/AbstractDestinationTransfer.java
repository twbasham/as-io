package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;

public abstract class AbstractDestinationTransfer implements
		IDestinationTransfer {

	private Logger log = LogFactory.getLog(AbstractDestinationTransfer.class);
	private Destination destination;
	private IInputStream in;
	private IOutputStream out;
	private Collection<Worker> workers = new ArrayList<Worker>();
	private ExecutorService executor;

	protected AbstractDestinationTransfer(Destination destination) {
		this.destination = destination;
	}

	@Override
	public Destination getDestination() {
		return destination;
	}

	@Override
	public void prepare() throws Exception {
		in = getInputStream(getInputStream());
		out = getOutputStream();
		for (int index = 0; index < getWorkerCount(); index++) {
			workers.add(new Worker(in, out));
		}
		executor = Executors.newFixedThreadPool(workers.size());
	}

	@Override
	public Long getPosition() {
		return in.getPosition();
	}

	@Override
	public Long size() {
		return in.size();
	}

	@Override
	public void stop() throws Exception {
		out.close();
		in.close();
	}

	@Override
	public void run() {
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
		Long limit = getLimit();
		if (limit == null) {
			return in;
		}
		return new LimitedInputStream(in, limit);
	}

	protected abstract int getWorkerCount();

	protected abstract Long getLimit();

	protected abstract IInputStream getInputStream() throws Exception;

	protected abstract IOutputStream getOutputStream();

	@Override
	public boolean isRunning() {
		return !executor.isTerminated();
	}

	@Override
	public String getName() {
		return destination.getName();
	}

}