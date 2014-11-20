package com.tibco.as.io;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.operation.GetOperation;
import com.tibco.as.io.operation.LoadOperation;
import com.tibco.as.io.operation.NoOperation;
import com.tibco.as.io.operation.PartialOperation;
import com.tibco.as.io.operation.PutOperation;
import com.tibco.as.io.operation.TakeOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.util.convert.IAccessor;
import com.tibco.as.util.convert.IConverter;
import com.tibco.as.util.log.LogFactory;

public class SpaceOutputStream implements IOutputStream {

	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;

	private Logger log = LogFactory.getLog(SpaceOutputStream.class);
	private IDestination destination;
	private IAccessor[] objectAccessors;
	private IAccessor[] tupleAccessors;
	private IConverter[] converters;
	private Space space;
	private IOperation operation;
	private long position;

	public SpaceOutputStream(IDestination destination) {
		this.destination = destination;
	}

	protected IDestination getDestination() {
		return destination;
	}

	@Override
	public synchronized void open() throws Exception {
		ImportConfig transfer = destination.getImportConfig();
		Metaspace metaspace = destination.getChannel().getMetaspace();
		String spaceName = destination.getSpaceDef().getName();
		SpaceDef spaceDef = metaspace.getSpaceDef(spaceName);
		if (spaceDef == null) {
			spaceDef = destination.getSpaceDef();
			Collection<String> keys = spaceDef.getKeyDef().getFieldNames();
			if (keys.isEmpty()) {
				log.log(Level.INFO, "No keys specified, using all fields");
				for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
					keys.add(fieldDef.getName());
				}
			}
			log.log(Level.FINE, "Defining {0}", spaceDef);
			metaspace.defineSpace(spaceDef);
		} else {
			destination.setSpaceDef(spaceDef);
		}
		objectAccessors = destination.getObjectAccessors(transfer);
		tupleAccessors = destination.getTupleAccessors(transfer);
		converters = destination.getJavaConverters(transfer);
		space = getSpace(metaspace);
		if (!space.isReady()) {
			long timeout = getWaitForReadyTimeout();
			log.log(Level.INFO,
					"Waiting for space ''{0}'' to become ready using timeout of {1} ms",
					new Object[] { space.getName(), timeout });
			space.waitForReady(timeout);
		}
		OperationType operationType = getOperationType();
		operation = getOperation(operationType, space);
	}

	private long getWaitForReadyTimeout() {
		Long timeout = destination.getImportConfig().getWaitForReadyTimeout();
		if (timeout == null) {
			return DEFAULT_WAIT_FOR_READY_TIMEOUT;
		}
		return timeout;
	}

	private Space getSpace(Metaspace metaspace) throws ASException {
		String spaceName = destination.getSpaceDef().getName();
		DistributionRole distributionRole = destination.getImportConfig()
				.getDistributionRole();
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
		OperationType operationType = destination.getImportConfig()
				.getOperation();
		if (operationType == null) {
			return OperationType.PUT;
		}
		return operationType;
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
		if (destination.getImportConfig().getDistributionRole() == DistributionRole.SEEDER) {
			return;
		}
		log.log(Level.FINE, "Closing space ''{0}''", space.getName());
		space.close();
		space = null;
	}

}
