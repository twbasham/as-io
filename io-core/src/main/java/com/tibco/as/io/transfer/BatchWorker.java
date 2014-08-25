package com.tibco.as.io.transfer;

import java.util.ArrayList;
import java.util.List;

import com.tibco.as.convert.IConverter;

public class BatchWorker<T, U> extends AbstractWorker<T, U> {

	private int batchSize;
	private List<U> elements;

	public BatchWorker(Executor<T, U> executor,
			IConverter<T, U> converter, int batchSize) {
		super(executor, converter);
		this.batchSize = batchSize;
		this.elements = new ArrayList<U>(batchSize);
	}

	@Override
	protected void write(U element) throws Exception {
		elements.add(element);
		if (elements.size() >= batchSize) {
			writeElements();
		}
	}

	private void writeElements() throws Exception {
		doWrite(elements);
		elements = new ArrayList<U>(batchSize);
	}

	@Override
	protected void flush() throws Exception {
		if (elements.isEmpty()) {
			return;
		}
		writeElements();
	}
}