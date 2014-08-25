package com.tibco.as.io.transfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tibco.as.io.EventManager;
import com.tibco.as.io.IInputStream;
import com.tibco.as.io.IOutputStream;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.ITransferListener;
import com.tibco.as.io.IWorker;
import com.tibco.as.io.TransferException;

public class Executor<T, U> implements ITransfer {

	private Collection<ITransferListener> listeners = new ArrayList<ITransferListener>();

	private boolean stopped;
	private IInputStream<T> in;
	private IOutputStream<U> out;
	private Collection<IWorker> workers = new ArrayList<IWorker>();
	private BlockingQueue<T> queue;
	private ExecutorService executor;

	public Executor(BlockingQueue<T> queue, IInputStream<T> in,
			IOutputStream<U> out) {
		this.queue = queue;
		this.in = in;
		this.out = out;
	}

	public BlockingQueue<T> getQueue() {
		return queue;
	}

	@Override
	public IInputStream<T> getInputStream() {
		return in;
	}

	@Override
	public void open() throws TransferException {
		executor = Executors.newFixedThreadPool(workers.size());
		try {
			in.open();
		} catch (Exception e) {
			throw new TransferException("Could not open input stream", e);
		}
		try {
			out.open();
		} catch (Exception e) {
			throw new TransferException("Could not open output stream", e);
		}
	}

	@Override
	public void close() throws TransferException {
		try {
			out.close();
		} catch (Exception e) {
			throw new TransferException("Could not close output stream", e);
		} finally {
			if (!stopped) {
				try {
					in.close();
				} catch (Exception e) {
					throw new TransferException("Could not close input stream",
							e);
				}
			}
		}
	}

	@Override
	public void execute() throws TransferException {
		for (ITransferListener listener : listeners) {
			listener.opened();
		}
		try {
			for (IWorker worker : workers) {
				executor.execute(worker);
			}
			T next;
			try {
				while (!stopped && (next = in.read()) != null) {
					queue.offer(next, 30, TimeUnit.SECONDS);
				}
			} catch (InterruptedException e) {
				throw new TransferException(
						"Interrupted while adding to work queue", e);
			} catch (Exception e) {
				throw new TransferException("Could not read input stream", e);
			}
		} finally {
			for (IWorker worker : workers) {
				worker.stop();
			}
			executor.shutdown();
			try {
				while (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
					EventManager.info("Waiting for executor to finish");
				}
			} catch (InterruptedException e) {
				throw new TransferException(
						"Interrupted while awaiting termination", e);
			}
		}
		for (ITransferListener listener : listeners) {
			listener.closed();
		}
	}

	@Override
	public void stop() throws TransferException {
		this.stopped = true;
		try {
			in.close();
		} catch (Exception e) {
			throw new TransferException("Could not stop transfer", e);
		}
	}

	@Override
	public long size() {
		return in.size();
	}

	public void run() {
		try {
			execute();
		} catch (Exception e) {
			EventManager.error(e, "Could not execute transfer");
		}
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return executor.awaitTermination(timeout, unit);
	}

	@Override
	public void addListener(ITransferListener listener) {
		listeners.add(listener);
	}

	public void write(List<U> elements) throws Exception {
		out.write(elements);
		for (ITransferListener listener : listeners) {
			listener.transferred(elements.size());
		}
	}

	public void write(U element) throws Exception {
		out.write(element);
		for (ITransferListener listener : listeners) {
			listener.transferred(1);
		}
	}

	public void addWorker(IWorker worker) {
		workers.add(worker);
	}

}
