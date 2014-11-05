package com.tibco.as.io;

public class DestinationImport extends AbstractDestinationTransfer {

	private Destination destination;

	public DestinationImport(Destination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	protected int getWorkerCount() {
		return destination.getImportWorkerCount();
	}

	@Override
	protected Long getLimit() {
		return destination.getImportLimit();
	}

	@Override
	protected IInputStream getInputStream() throws Exception {
		return destination.getInputStream();
	}

	@Override
	protected IOutputStream getOutputStream() {
		Integer batchSize = destination.getSpaceBatchSize();
		if (batchSize == null) {
			return new SpaceOutputStream(destination);
		}
		return new BatchSpaceOutputStream(destination, batchSize);
	}

}
