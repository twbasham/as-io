package com.tibco.as.io;

import com.tibco.as.convert.IAccessor;
import com.tibco.as.convert.IConverter;

public abstract class AbstractOutputStream<T> implements IOutputStream {

	private Destination destination;
	private IAccessor[] objectAccessors;
	private IAccessor[] tupleAccessors;
	private ThreadLocal<IConverter[]> context = new ThreadLocal<IConverter[]>();

	protected AbstractOutputStream(Destination destination) {
		this.destination = destination;
	}

	@Override
	public synchronized void open() throws Exception {
		context.set(destination.getFieldConverters());
		if (tupleAccessors == null) {
			tupleAccessors = destination.getTupleAccessors();
		}
		if (objectAccessors == null) {
			objectAccessors = destination.getObjectAccessors();
		}
	}

	@Override
	public synchronized void close() throws Exception {
		context.remove();
	}

	@Override
	public void write(Object tuple) throws Exception {
		IConverter[] converters = context.get();
		T array = newObject(tupleAccessors.length);
		for (int index = 0; index < tupleAccessors.length; index++) {
			if (tupleAccessors[index] == null) {
				continue;
			}
			if (converters[index] == null) {
				continue;
			}
			if (objectAccessors[index] == null) {
				continue;
			}
			Object value = tupleAccessors[index].get(tuple);
			if (value == null) {
				continue;
			}
			Object converted = converters[index].convert(value);
			if (converted == null) {
				continue;
			}
			objectAccessors[index].set(array, converted);
		}
		doWrite(array);
	}

	protected abstract void doWrite(T array) throws Exception;

	protected abstract T newObject(int length);

}
