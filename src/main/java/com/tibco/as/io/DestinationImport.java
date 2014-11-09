package com.tibco.as.io;

public class DestinationImport extends AbstractDestinationTransfer {

	private Destination destination;

	public DestinationImport(Destination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	protected TransferConfig getConfig() {
		return destination.getImportConfig();
	}

	@Override
	protected IInputStream getInputStream() throws Exception {
		return destination.getInputStream();
	}

	@Override
	protected IOutputStream getOutputStream() {
		Integer batchSize = destination.getImportConfig().getBatchSize();
		if (batchSize == null || batchSize == 1) {
			return new SpaceOutputStream(destination);
		}
		return new BatchSpaceOutputStream(destination, batchSize);
	}

}
