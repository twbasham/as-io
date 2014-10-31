package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.IOperation;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.impl.data.ASSpaceResultList;

public class NoOperation implements IOperation {

	@Override
	public Tuple execute(Tuple tuple) {
		return tuple;
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return new ASSpaceResultList();
	}

}
