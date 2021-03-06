package com.tibco.as.io.operation;

import java.text.MessageFormat;
import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class PutOperation extends AbstractOperation {

	private PutOptions options;

	public PutOperation(Space space, long timeout, boolean keepOpen, PutOptions options) {
		super(space, timeout, keepOpen);
		this.options = options;
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return space.put(tuple, options);
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		return space.putAll(tuples, options);
	}

	@Override
	public String toString() {
		return MessageFormat.format("put {0}", options);
	}

}
