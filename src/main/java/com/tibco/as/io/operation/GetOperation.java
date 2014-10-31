package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.IOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.GetOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class GetOperation implements IOperation {

	private GetOptions options = GetOptions.create();
	private Space space;

	public GetOperation(Space space) {
		this.space = space;
	}

	@Override
	public Tuple execute(Tuple tuple) throws ASException {
		return space.get(tuple, options);
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return space.getAll(tuples);
	}

}
