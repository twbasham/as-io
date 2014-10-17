package com.tibco.as.io;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.operation.GetOperation;
import com.tibco.as.io.operation.IOperation;
import com.tibco.as.io.operation.LoadOperation;
import com.tibco.as.io.operation.NoOperation;
import com.tibco.as.io.operation.PartialOperation;
import com.tibco.as.io.operation.PutOperation;
import com.tibco.as.io.operation.TakeOperation;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class SpaceOutputStream implements IOutputStream {

	private Logger log = LogFactory.getLog(SpaceOutputStream.class);
	private Metaspace metaspace;
	private DestinationConfig config;
	private IOperation operation;
	private long position;

	public SpaceOutputStream(Metaspace metaspace, DestinationConfig config) {
		this.metaspace = metaspace;
		this.config = config;
	}

	@Override
	public void open() throws Exception {
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		if (spaceDef == null) {
			spaceDef = config.getSpaceDef();
			log.log(Level.FINE, "Defining ", spaceDef);
			metaspace.defineSpace(spaceDef);
		}
		operation = getOperation(metaspace);
		operation.open();
	}

	private IOperation getOperation(Metaspace metaspace) throws ASException {
		switch (getOperationType()) {
		case GET:
			return new GetOperation(metaspace, config);
		case LOAD:
			return new LoadOperation(metaspace, config);
		case NONE:
			return new NoOperation();
		case PARTIAL:
			return new PartialOperation(metaspace, config);
		case PUT:
			return new PutOperation(metaspace, config);
		case TAKE:
			return new TakeOperation(metaspace, config);
		}
		throw new RuntimeException("Invalid operation");
	}

	private OperationType getOperationType() {
		if (config.getOperation() == null) {
			return OperationType.PUT;
		}
		return config.getOperation();
	}

	@Override
	public void write(Object tuple) throws ASException {
		position += execute(operation, (Tuple) tuple);
	}

	protected int execute(IOperation operation, Tuple tuple) throws ASException {
		operation.execute(tuple);
		return 1;
	}

	public long getPosition() {
		return position;
	}

	@Override
	public void close() throws ASException {
		close(operation);
	}

	protected void close(IOperation operation) throws ASException {
		operation.close();
	}

}
