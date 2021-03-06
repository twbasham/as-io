package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class LoadOperation extends AbstractOperation {

	public LoadOperation(Space space, long timeout, boolean keepOpen) {
		super(space, timeout, keepOpen);
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		space.load(tuple);
		return tuple;
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		return space.loadAll(tuples);
	}

	@Override
	public String toString() {
		return "load";
	}

}