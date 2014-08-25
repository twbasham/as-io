package com.tibco.as.io.operation;

import java.text.MessageFormat;
import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.TakeOptions;
import com.tibco.as.space.Tuple;

public class TakeOperation extends AbstractOperation {

	TakeOptions options;

	public TakeOperation(Metaspace metaspace, String spaceName,
			TakeOptions options) {
		super(metaspace, spaceName);
		this.options = options;
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return space.take(tuple, options);
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		return space.takeAll(tuples, options);
	}

	@Override
	public String toString() {
		return MessageFormat.format("take {0}", options);
	}

}
