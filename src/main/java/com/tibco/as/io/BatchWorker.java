package com.tibco.as.io;

import java.util.ArrayList;
import java.util.List;

import com.tibco.as.convert.IConverter;

public class BatchWorker<T, U> extends Worker<T, U> {

	private int batchSize;
	private List<U> elements;
	private IOutputStream<U> out;

	public BatchWorker(IInputStream<T> in, IConverter<T, U> converter,
			IOutputStream<U> out, int batchSize) {
		super(in, converter, out);
		this.out = out;
		this.batchSize = batchSize;
		this.elements = new ArrayList<U>(batchSize);
	}

	@Override
	protected void write(U element) throws Exception {
		elements.add(element);
		if (elements.size() >= batchSize) {
			write();
		}
	}

	@Override
	protected void close() throws Exception {
		if (!isClosed() && !elements.isEmpty()) {
			write();
		}
		super.close();
	}

	private void write() throws Exception {
		out.write(elements);
		elements = new ArrayList<U>(batchSize);
	}
}