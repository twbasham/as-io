package com.tibco.as.io.work;

import java.util.ArrayList;
import java.util.List;

import com.tibco.as.convert.IConverter;
import com.tibco.as.io.IInputStream;
import com.tibco.as.io.IOutputStream;

public class BatchWorker<T, U> extends AbstractWorker<T, U> {

	private int batchSize;
	private List<U> elements;
	private IOutputStream<U> out;

	public BatchWorker(IInputStream<T> in, IConverter<T, U> converter,
			IOutputStream<U> out, int batchSize) {
		super(in, converter);
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
	protected void execute() throws Exception {
		super.execute();
		if (isClosed() || elements.isEmpty()) {
			return;
		}
		write();
	}

	private void write() throws Exception {
		out.write(elements);
		elements = new ArrayList<U>(batchSize);
	}
}