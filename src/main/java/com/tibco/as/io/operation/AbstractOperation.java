package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.EventManager;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public abstract class AbstractOperation implements IOperation {

	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;

	private Space space;

	private boolean keepSpaceOpen;

	private boolean closed;

	private Metaspace metaspace;

	private String spaceName;

	private DistributionRole distributionRole;

	private Long waitForReadyTimeout;

	public AbstractOperation(Metaspace metaspace, String spaceName) {
		this.metaspace = metaspace;
		this.spaceName = spaceName;
	}

	public DistributionRole getDistributionRole() {
		return distributionRole;
	}

	@Override
	public void setDistributionRole(DistributionRole distributionRole) {
		this.distributionRole = distributionRole;
	}

	public boolean isKeepSpaceOpen() {
		return keepSpaceOpen;
	}

	@Override
	public void setKeepSpaceOpen(boolean keepSpaceOpen) {
		this.keepSpaceOpen = keepSpaceOpen;
	}

	private long getWaitForReadyTimeout() {
		if (waitForReadyTimeout == null) {
			return DEFAULT_WAIT_FOR_READY_TIMEOUT;
		}
		return waitForReadyTimeout;
	}

	@Override
	public void setWaitForReadyTimeout(Long timeout) {
		this.waitForReadyTimeout = timeout;
	}

	public void open() throws Exception {
		if (distributionRole == null) {
			space = metaspace.getSpace(spaceName);
		} else {
			space = metaspace.getSpace(spaceName, distributionRole);
		}
		if (space.isReady()) {
			return;
		}
		Long timeout = getWaitForReadyTimeout();
		EventManager
				.info("Waiting until space is ready using timeout of {0} ms",
						timeout);
		space.waitForReady(timeout);
	}

	@Override
	public Tuple execute(Tuple tuple) throws ASException {
		return execute(space, tuple);
	}

	@Override
	public SpaceResultList execute(Collection<Tuple> tuples) {
		return execute(space, tuples);
	}

	protected abstract Tuple execute(Space space, Tuple tuple)
			throws ASException;

	protected abstract SpaceResultList execute(Space space,
			Collection<Tuple> tuples);

	@Override
	public void close() throws ASException {
		if (isClosed()) {
			return;
		}
		if (!keepSpaceOpen) {
			if (space == null) {
				return;
			}
			space.close();
			space = null;
		}
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

}
