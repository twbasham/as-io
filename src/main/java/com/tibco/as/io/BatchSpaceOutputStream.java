package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.operation.IOperation;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceResult;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class BatchSpaceOutputStream extends SpaceOutputStream {

	private Logger log = LogFactory.getLog(BatchSpaceOutputStream.class);
	private int batchSize;
	private Collection<Tuple> tuples;

	public BatchSpaceOutputStream(Metaspace metaspace,
			DestinationConfig config, int batchSize) {
		super(metaspace, config);
		this.batchSize = batchSize;
	}

	@Override
	public void open() throws Exception {
		this.tuples = new ArrayList<Tuple>(batchSize);
		super.open();
	}

	@Override
	protected void close(IOperation operation) throws ASException {
		if (!tuples.isEmpty()) {
			execute(operation);
		}
		super.close(operation);
	}

	@Override
	protected int execute(IOperation operation, Tuple tuple) throws ASException {
		tuples.add((Tuple) tuple);
		if (tuples.size() >= batchSize) {
			return execute(operation);
		}
		return 0;
	}

	private int execute(IOperation operation) {
		int count = 0;
		SpaceResultList resultList = operation.execute(tuples);
		if (resultList.hasError()) {
			Iterator<SpaceResult> resultIterator = resultList.iterator();
			while (resultIterator.hasNext()) {
				SpaceResult result = resultIterator.next();
				if (result.hasError()) {
					log.log(Level.SEVERE, "Could not perform operation",
							result.getError());
				} else {
					count++;
				}
			}
		} else {
			count = tuples.size();
		}
		tuples = new ArrayList<Tuple>(batchSize);
		return count;
	}

}
