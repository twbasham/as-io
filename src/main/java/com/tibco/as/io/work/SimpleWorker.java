package com.tibco.as.io.work;

import com.tibco.as.convert.IConverter;
import com.tibco.as.io.IInputStream;
import com.tibco.as.io.IOutputStream;

public class SimpleWorker<T, U> extends AbstractWorker<T, U> {

	private IOutputStream<U> out;

	public SimpleWorker(IInputStream<T> in, IConverter<T, U> converter,
			IOutputStream<U> out) {
		super(in, converter);
		this.out = out;
	}

	@Override
	protected void write(U element) throws Exception {
		out.write(element);
	}

}