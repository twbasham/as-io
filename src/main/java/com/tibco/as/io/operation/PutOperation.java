package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.DestinationConfig;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class PutOperation extends AbstractOperation {

	private PutOptions options = PutOptions.create().setForget(true);

	public PutOperation(Metaspace metaspace, DestinationConfig config) {
		super(metaspace, config);
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return space.put(tuple, options);
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		return space.putAll(tuples, options);
	}

}
