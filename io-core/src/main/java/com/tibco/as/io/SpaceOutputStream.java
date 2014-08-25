package com.tibco.as.io;

import java.util.Iterator;
import java.util.List;

import com.tibco.as.io.operation.GetOperation;
import com.tibco.as.io.operation.IOperation;
import com.tibco.as.io.operation.LoadOperation;
import com.tibco.as.io.operation.NoOperation;
import com.tibco.as.io.operation.PartialOperation;
import com.tibco.as.io.operation.PutOperation;
import com.tibco.as.io.operation.TakeOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.GetOptions;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.SpaceResult;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.TakeOptions;
import com.tibco.as.space.Tuple;

public class SpaceOutputStream implements IOutputStream<Tuple> {

	private IOperation operation;

	private long position;

	private Metaspace metaspace;

	private String spaceName;

	private Import config;

	public SpaceOutputStream(Metaspace metaspace, String spaceName,
			Import config) {
		this.metaspace = metaspace;
		this.spaceName = spaceName;
		this.config = config;
	}

	@Override
	public void open() throws Exception {
		operation = getOperation(metaspace, spaceName, config.getOperation());
		DistributionRole distributionRole = config.getDistributionRole();
		operation.setDistributionRole(distributionRole);
		Boolean keepSpaceOpen = config.isKeepSpaceOpen();
		if (keepSpaceOpen == null) {
			keepSpaceOpen = distributionRole == DistributionRole.SEEDER;
		}
		operation.setKeepSpaceOpen(keepSpaceOpen);
		operation.setWaitForReadyTimeout(config.getWaitForReadyTimeout());
		operation.open();
	}

	private IOperation getOperation(Metaspace metaspace, String spaceName,
			Operation operation) {
		if (operation == null) {
			operation = Operation.PUT;
		}
		switch (operation) {
		case GET:
			return new GetOperation(metaspace, spaceName, GetOptions.create());
		case LOAD:
			return new LoadOperation(metaspace, spaceName);
		case NONE:
			return new NoOperation();
		case PARTIAL:
			return new PartialOperation(metaspace, spaceName, PutOptions
					.create().setForget(true));
		case PUT:
			return new PutOperation(metaspace, spaceName, PutOptions.create()
					.setForget(true));
		case TAKE:
			return new TakeOperation(metaspace, spaceName, TakeOptions.create()
					.setForget(true));
		}
		throw new RuntimeException("Invalid operation");
	}

	@Override
	public void write(List<Tuple> tuples) {
		SpaceResultList resultList = operation.execute(tuples);
		if (resultList.hasError()) {
			Iterator<SpaceResult> resultIterator = resultList.iterator();
			while (resultIterator.hasNext()) {
				SpaceResult spaceResult = resultIterator.next();
				if (spaceResult.hasError()) {
					EventManager.error(spaceResult.getError(), spaceName);
				} else {
					position++;
				}
			}
		} else {
			position += tuples.size();
		}
	}

	@Override
	public void write(Tuple tuple) throws ASException {
		operation.execute(tuple);
		position++;
	}

	public long getPosition() {
		return position;
	}

	@Override
	public void close() throws ASException {
		operation.close();
	}

	@Override
	public boolean isClosed() {
		return operation.isClosed();
	}

}
