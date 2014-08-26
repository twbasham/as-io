package com.tibco.as.io.operation;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class PartialOperation extends AbstractOperation {

	private PutOptions options;

	public PartialOperation(Metaspace metaspace, String spaceName,
			PutOptions options) {
		super(metaspace, spaceName);
		this.options = options;
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return space.put(update(space, tuple), options);
	}

	private Tuple update(Space space, Tuple tuple) throws ASException {
		Tuple existing = space.get(tuple);
		if (existing == null) {
			return tuple;
		}
		existing.putAll(tuple);
		return existing;
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		Collection<Tuple> updates = new ArrayList<Tuple>();
		for (Tuple tuple : tuples) {
			try {
				updates.add(update(space, tuple));
			} catch (ASException e) {
				e.printStackTrace();
			}
		}
		return space.putAll(updates, options);
	}

	@Override
	public String toString() {
		return MessageFormat.format("put {0}", options);
	}

}
