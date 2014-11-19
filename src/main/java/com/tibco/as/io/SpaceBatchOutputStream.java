package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.space.ASException;
import com.tibco.as.space.SpaceResult;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;
import com.tibco.as.util.log.LogFactory;

public class SpaceBatchOutputStream extends SpaceOutputStream {

	private static final ThreadLocal<Collection<Tuple>> context = new ThreadLocal<Collection<Tuple>>();

	private Logger log = LogFactory.getLog(SpaceBatchOutputStream.class);
	private Integer batchSize;

	public SpaceBatchOutputStream(IDestination destination) {
		super(destination);
	}

	@Override
	public synchronized void open() throws Exception {
		this.batchSize = getDestination().getImportConfig().getBatchSize();
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
