package com.tibco.as.io.operation;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.io.IOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class PartialOperation implements IOperation {

	private PutOptions options = PutOptions.create().setForget(true);
	private Space space;

	public PartialOperation(Space space) {
		this.space = space;
	}

	@Override
	public Tuple execute(Tuple tuple) throws ASException {
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
	public SpaceResultList execute(Collection<Tuple> tuples) {
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

}
