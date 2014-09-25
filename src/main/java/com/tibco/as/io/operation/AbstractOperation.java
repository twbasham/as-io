package com.tibco.as.io.operation;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public abstract class AbstractOperation implements IOperation {

	private Logger log = LogFactory.getLog(AbstractOperation.class);

	private Space space;
	private boolean keepOpen;
	private boolean closed;
	private long timeout;

	public AbstractOperation(Space space, long timeout, boolean keepOpen) {
		this.space = space;
		this.timeout = timeout;
		this.keepOpen = keepOpen;
	}

	public void open() throws ASException {
		if (space.isReady()) {
			return;
		}
		log.log(Level.INFO, "Waiting {0} ms for space ''{1}'' readiness",
				new Object[] { timeout, space.getName() });
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
		if (!keepOpen) {
			if (space == null) {
				return;
			}
			space.close();
			space = null;
		}
		closed = true;
	}

	public boolean isClosed() {
		return closed;
	}

}
