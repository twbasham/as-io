package com.tibco.as.io.operation;

import java.text.MessageFormat;
import java.util.Collection;

import com.tibco.as.io.IOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.TakeOptions;
import com.tibco.as.space.Tuple;

public class TakeOperation implements IOperation {

	private TakeOptions options = TakeOptions.create().setForget(true);
	private Space space;

	public TakeOperation(Space space) {
		this.space = space;
	}

	@Override
	public Tuple execute(Tuple tuple) throws ASException {
		return space.take(tuple, options);
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return space.takeAll(tuples, options);
	}

	@Override
	public String toString() {
		return MessageFormat.format("take {0}", options);
	}

}
