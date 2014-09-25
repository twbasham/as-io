package com.tibco.as.io;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.operation.IOperation;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.SpaceResult;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class SpaceOutputStream implements IOutputStream<Tuple> {

	private Logger log = LogFactory.getLog(SpaceOutputStream.class);

	private IOperation operation;
	private long position;

	public SpaceOutputStream(IOperation operation) {
		this.operation = operation;
	}

	@Override
	public void open() throws Exception {
		operation.open();
	}

	@Override
	public void write(List<Tuple> tuples) {
		SpaceResultList resultList = operation.execute(tuples);
		if (resultList.hasError()) {
			Iterator<SpaceResult> resultIterator = resultList.iterator();
			while (resultIterator.hasNext()) {
				SpaceResult result = resultIterator.next();
				if (result.hasError()) {
					log.log(Level.SEVERE, "Could not perform operation",
							result.getError());
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

}
