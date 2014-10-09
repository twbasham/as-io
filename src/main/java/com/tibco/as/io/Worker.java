package com.tibco.as.io;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.ConvertException;
import com.tibco.as.convert.IConverter;
import com.tibco.as.log.LogFactory;

public class Worker<T, U> implements Runnable {

	private Logger log = LogFactory.getLog(Worker.class);

	private IInputStream<T> in;
	private IConverter<T, U> converter;
	private IOutputStream<U> out;

	public Worker(IInputStream<T> in, IConverter<T, U> converter,
			IOutputStream<U> out) {
		this.in = in;
		this.converter = converter;
		this.out = out;
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
		open();
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
		close();
	}

	protected void open() throws Exception {
		out.open();
	}

	protected void close() throws Exception {
		out.close();
	}

	protected void write(U element) throws Exception {
		out.write(element);
	}

	protected boolean isClosed() {
		return in.isClosed();
	}

}
