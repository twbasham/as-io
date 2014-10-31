package com.tibco.as.io;

public class Import extends AbstractTransfer {

	public Import(Destination destination) {
		super(destination);
	}

	@Override
	protected IInputStream getInputStream(Destination destination) {
		IInputStream inputStream = destination.getInputStream();
		Long limit = destination.getImportLimit();
		if (limit == null) {
			return inputStream;
		}
		return new LimitedInputStream(inputStream, limit);
	}

	@Override
	protected IOutputStream getOutputStream(Destination destination) {
		Integer batchSize = destination.getSpaceBatchSize();
		if (batchSize == null) {
			return new SpaceOutputStream(destination);
		}
		return new BatchSpaceOutputStream(destination, batchSize);
	}

	@Override
	protected int getWorkerCount(Destination destination) {
		return destination.getImportWorkerCount();
	}

}
