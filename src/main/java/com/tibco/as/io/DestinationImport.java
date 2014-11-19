package com.tibco.as.io;

public class DestinationImport extends AbstractDestinationTransfer {

	private IDestination destination;

	public DestinationImport(IDestination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	public TransferConfig getConfig() {
		return destination.getImportConfig();
	}

	@Override
	protected IInputStream getInputStream() {
		return destination.getInputStream();
	}

	@Override
	protected IOutputStream getOutputStream() {
		if (destination.getImportConfig().isBatch()) {
			return new SpaceBatchOutputStream(destination);
		}
		return new SpaceOutputStream(destination);
	}

}
