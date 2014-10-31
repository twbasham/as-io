package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.IOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class LoadOperation implements IOperation {

	private Space space;

	public LoadOperation(Space space) {
		this.space = space;
	}

	@Override
	public Tuple execute(Tuple tuple) throws ASException {
		space.load(tuple);
		return tuple;
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return space.loadAll(tuples);
	}

}