package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.SpaceResult;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public class BatchSpaceOutputStream extends SpaceOutputStream {

	private Logger log = LogFactory.getLog(BatchSpaceOutputStream.class);
	private int batchSize;
	private static final ThreadLocal<Collection<Tuple>> context = new ThreadLocal<Collection<Tuple>>();

	public BatchSpaceOutputStream(Destination destination, int batchSize) {
		super(destination);
		this.batchSize = batchSize;
	}

	@Override
	public synchronized void open() throws Exception {
		initializeTuples();
		super.open();
	}

	private void initializeTuples() {
		context.set(new ArrayList<Tuple>(batchSize));
	}

	@Override
	public synchronized void close() throws ASException {
		Collection<Tuple> tuples = context.get();
		if (!tuples.isEmpty()) {
			execute(tuples);
		}
		super.close();
	}

	@Override
	protected int write(Tuple tuple) throws ASException {
		Collection<Tuple> tuples = context.get();
		if (tuples.size() == batchSize) {
			execute(tuples);
		}
		return 0;
	}

	private int execute(Collection<Tuple> tuples) {
		int count = 0;
		SpaceResultList resultList = getOperation().execute(tuples);
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
		initializeTuples();
		return count;
	}

}
