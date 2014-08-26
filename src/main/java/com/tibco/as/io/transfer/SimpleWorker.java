package com.tibco.as.io.transfer;

import com.tibco.as.convert.IConverter;

public class SimpleWorker<T, U> extends AbstractWorker<T, U> {

	public SimpleWorker(Executor<T, U> executor,
			IConverter<T, U> converter) {
		super(executor, converter);
	}

	@Override
	protected void flush() throws Exception {
		// do nothing
	}

	@Override
	protected void write(U element) throws Exception {
		doWrite(element);
	}

}