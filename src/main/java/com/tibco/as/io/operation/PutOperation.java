package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.IOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class PutOperation implements IOperation {

	private PutOptions options = PutOptions.create().setForget(true);
	private Space space;

	public PutOperation(Space space) {
		this.space = space;
	}

	@Override
	public Tuple execute(Tuple tuple) throws ASException {
		return space.put(tuple, options);
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return space.putAll(tuples, options);
	}

}
