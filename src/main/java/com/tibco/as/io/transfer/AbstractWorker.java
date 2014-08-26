package com.tibco.as.io.transfer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.tibco.as.convert.ConvertException;
import com.tibco.as.convert.IConverter;
import com.tibco.as.io.EventManager;
import com.tibco.as.io.IWorker;

public abstract class AbstractWorker<T, U> implements IWorker {

	private Executor<T, U> executor;
	private IConverter<T, U> converter;
	private boolean stopped;

	public AbstractWorker(Executor<T, U> executor,
			IConverter<T, U> converter) {
		this.executor = executor;
		this.converter = converter;
	}

	@Override
	public void run() {
		BlockingQueue<T> queue = executor.getQueue();
		T element;
		try {
			while (!((element = queue.poll(100, TimeUnit.MILLISECONDS)) == null && stopped)) {
				if (element != null) {
					try {
						U converted = converter.convert(element);
						if (converted != null) {
							try {
								write(converted);
							} catch (Exception e) {
								EventManager.error(e,
										"Could not write to output");
							}
						}
					} catch (ConvertException e) {
						EventManager.warn(e);
					}
				}
			}
			try {
				flush();
			} catch (Exception e) {
				EventManager.error(e, "Could not flush");
			}
		} catch (InterruptedException e) {
			EventManager.info("Worker interrupted");
		}
	}

	protected abstract void flush() throws Exception;

	@Override
	public void stop() {
		stopped = true;
	}

	protected void doWrite(List<U> elements) throws Exception {
		executor.write(elements);
	}

	protected void doWrite(U element) throws Exception {
		executor.write(element);
	}

	protected abstract void write(U element) throws Exception;

}
