package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.DestinationConfig;
import com.tibco.as.space.ASException;
import com.tibco.as.space.GetOptions;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class GetOperation extends AbstractOperation {

	private GetOptions options = GetOptions.create();

	public GetOperation(Metaspace metaspace, DestinationConfig config) {
		super(metaspace, config);
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return space.get(tuple, options);
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		return space.getAll(tuples);
	}

}
