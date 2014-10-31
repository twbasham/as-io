package com.tibco.as.io;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.IAccessor;
import com.tibco.as.convert.IConverter;
import com.tibco.as.io.operation.GetOperation;
import com.tibco.as.io.operation.LoadOperation;
import com.tibco.as.io.operation.NoOperation;
import com.tibco.as.io.operation.PartialOperation;
import com.tibco.as.io.operation.PutOperation;
import com.tibco.as.io.operation.TakeOperation;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class SpaceOutputStream implements IOutputStream {

	private Logger log = LogFactory.getLog(SpaceOutputStream.class);
	private Destination destination;
	private IAccessor[] objectAccessors;
	private IAccessor[] tupleAccessors;
	private IConverter[] converters;
	private Space space;
	private IOperation operation;
	private long position;

	public SpaceOutputStream(Destination destination) {
		this.destination = destination;
	}

	@Override
	public synchronized void open() throws Exception {
		Metaspace metaspace = destination.getChannel().getMetaspace();
		SpaceDef spaceDef = metaspace.getSpaceDef(destination.getSpace());
		if (spaceDef == null) {
			spaceDef = destination.getSpaceDef();
			log.log(Level.FINE, "Defining {0}", spaceDef);
			metaspace.defineSpace(spaceDef);
		}
		destination.setSpaceDef(spaceDef);
		objectAccessors = destination.getObjectAccessors();
		tupleAccessors = destination.getTupleAccessors();
		converters = destination.getJavaConverters();
		space = getSpace(metaspace);
		if (!space.isReady()) {
			long timeout = destination.getWaitForReadyTimeout();
			log.log(Level.INFO,
					"Waiting for space ''{0}'' to become ready using timeout of {1} ms",
					new Object[] { space.getName(), timeout });
			space.waitForReady(timeout);
		}
		OperationType operationType = getOperationType();
		operation = getOperation(operationType, space);
	}

	private Space getSpace(Metaspace metaspace) throws ASException {
		String spaceName = destination.getSpace();
		DistributionRole distributionRole = destination.getDistributionRole();
		if (distributionRole == null) {
			log.log(Level.FINE, "Joining space ''{0}''", spaceName);
			return metaspace.getSpace(spaceName);
		}
		log.log(Level.FINE, "Joining space ''{0}'' as {1}", new Object[] {
				spaceName, distributionRole });
		return metaspace.getSpace(spaceName, distributionRole);
	}

	protected IOperation getOperation() {
		return operation;
	}

	private IOperation getOperation(OperationType type, Space space)
			throws ASException {
		switch (type) {
		case GET:
			return new GetOperation(space);
		case LOAD:
			return new LoadOperation(space);
		case PARTIAL:
			return new PartialOperation(space);
		case PUT:
			return new PutOperation(space);
		case TAKE:
			return new TakeOperation(space);
		default:
			return new NoOperation();
		}
	}

	private OperationType getOperationType() {
		if (destination.getOperation() == null) {
			return OperationType.PUT;
		}
		return destination.getOperation();
	}

	@Override
	public void write(Object object) throws Exception {
		Tuple tuple = Tuple.create();
		for (int index = 0; index < objectAccessors.length; index++) {
			if (objectAccessors[index] == null) {
				continue;
			}
			if (converters[index] == null) {
				continue;
			}
			if (tupleAccessors[index] == null) {
				continue;
			}
			Object value = objectAccessors[index].get(object);
			if (value == null) {
				continue;
			}
			Object converted = converters[index].convert(value);
			if (converted == null) {
				continue;
			}
			tupleAccessors[index].set(tuple, converted);
		}
		position += write(tuple);
	}

	protected int write(Tuple tuple) throws ASException {
		operation.execute(tuple);
		return 1;
	}

	public long getPosition() {
		return position;
	}

	@Override
	public synchronized void close() throws ASException {
		if (space == null) {
			return;
		}
		if (destination.getDistributionRole() == DistributionRole.SEEDER) {
			return;
		}
		log.log(Level.FINE, "Closing space ''{0}''", space.getName());
		space.close();
		space = null;
	}

}
