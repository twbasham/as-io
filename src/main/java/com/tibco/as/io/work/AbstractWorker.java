package com.tibco.as.io.work;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.ConvertException;
import com.tibco.as.convert.IConverter;
import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;

public abstract class AbstractWorker<T, U> implements Runnable {

	private Logger log = LogFactory.getLog(AbstractWorker.class);

	private IInputStream<T> in;
	private IConverter<T, U> converter;

	public AbstractWorker(IInputStream<T> in, IConverter<T, U> converter) {
		this.in = in;
		this.converter = converter;
	}

	@Override
	public void run() {
		try {
			execute();
		} catch (InterruptedException e) {
			log.log(Level.INFO, "Interrupted");
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not execute", e);
		}
	}

	protected void execute() throws Exception {
		log.log(Level.FINE, "Worker starting in thread ''{0}''", Thread
				.currentThread().getName());
		T element;
		while ((element = in.read()) != null) {
			try {
				U converted = converter.convert(element);
				if (converted != null) {
					try {
						write(converted);
					} catch (Exception e) {
						log.log(Level.SEVERE, "Could not write to output", e);
					}
				}
			} catch (ConvertException e) {
				log.log(Level.SEVERE, "Could not convert", e);
			}
		}
		log.log(Level.FINE, "Worker finished in thread ''{0}''", Thread
				.currentThread().getName());
	}

	protected abstract void write(U element) throws Exception;

	protected boolean isClosed() {
		return in.isClosed();
	}

}
