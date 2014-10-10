package com.tibco.as.io.operation;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.DestinationConfig;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public abstract class AbstractOperation implements IOperation {

	private Logger log = LogFactory.getLog(AbstractOperation.class);
	private Metaspace metaspace;
	private DestinationConfig config;
	private Space space;

	public AbstractOperation(Metaspace metaspace, DestinationConfig config) {
		this.metaspace = metaspace;
		this.config = config;
	}

	private Space getSpace(Metaspace metaspace) throws ASException {
		String spaceName = config.getSpace();
		if (config.getDistributionRole() == null) {
			return metaspace.getSpace(spaceName);
		} else {
			return metaspace.getSpace(spaceName, config.getDistributionRole());
		}
	}

	public void open() throws ASException {
		this.space = getSpace(metaspace);
		long timeout = config.getWaitForReadyTimeout();
		if (space.isReady()) {
			return;
		}
		log.log(Level.INFO, "Waiting {0} ms for space ''{1}'' to become ready",
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
		if (space == null) {
			return;
		}
		if (config.getDistributionRole() != DistributionRole.SEEDER) {
			space.close();
			space = null;
		}
	}

}
