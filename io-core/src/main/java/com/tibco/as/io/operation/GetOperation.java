package com.tibco.as.io.operation;

import java.text.MessageFormat;
import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.GetOptions;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class GetOperation extends AbstractOperation {

	private GetOptions options;

	public GetOperation(Metaspace metaspace, String spaceName,
			GetOptions options) {
		super(metaspace, spaceName);
		this.options = options;
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return space.get(tuple, options);
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		return space.getAll(tuples);
	}

	@Override
	public String toString() {
		return MessageFormat.format("get {0}", options);
	}

}
