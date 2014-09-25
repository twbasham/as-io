package com.tibco.as.io;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tibco.as.convert.IConverter;
import com.tibco.as.io.work.AbstractWorker;
import com.tibco.as.io.work.BatchWorker;
import com.tibco.as.io.work.SimpleWorker;

public class Transfer<T, U> implements ITransfer {

	private IInputStream<T> in;
	private Collection<IConverter<T, U>> converters;
	private IOutputStream<U> out;
	private int batchSize;
	private ExecutorService service;

	public Transfer(IInputStream<T> in,
			Collection<IConverter<T, U>> converters, IOutputStream<U> out,
			int batchSize) {
		this.in = in;
		this.converters = converters;
		this.out = out;
		this.batchSize = batchSize;
	}

	@Override
	public void open() throws Exception {
		in.open();
		out.open();
		service = Executors.newFixedThreadPool(converters.size());
		for (IConverter<T, U> converter : converters) {
			service.execute(createWorker(converter));
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
		try {
			if (in != null) {
				in.close();
			}
			in = null;
		} finally {
			if (out != null) {
				out.close();
			}
			out = null;
		}
	}

	private AbstractWorker<T, U> createWorker(IConverter<T, U> converter)
			throws Exception {
		if (batchSize > 1) {
			return new BatchWorker<T, U>(in, converter, out, batchSize);
		}
		return new SimpleWorker<T, U>(in, converter, out);
	}

	@Override
	public void stop() throws Exception {
		if (in == null) {
			return;
		}
		in.close();
	}
}
