package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.impl.data.ASSpaceResultList;

public class NoOperation implements IOperation {

	private boolean closed;

	@Override
	public void open() throws Exception {
		closed = false;
	}

	@Override
	public Tuple execute(Tuple tuple) {
		return tuple;
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return new ASSpaceResultList();
	}

	@Override
	public String toString() {
		return "no-op";
	}

	@Override
	public void close() {
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public void setDistributionRole(DistributionRole distributionRole) {
	}

	@Override
	public void setKeepSpaceOpen(boolean keepSpaceOpen) {
	}

	@Override
	public void setWaitForReadyTimeout(Long waitTimeout) {
	}

}
