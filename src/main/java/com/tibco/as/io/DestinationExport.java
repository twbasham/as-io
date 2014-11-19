package com.tibco.as.io;

public class DestinationExport extends AbstractDestinationTransfer {

	private IDestination destination;

	public DestinationExport(IDestination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	public TransferConfig getConfig() {
		return destination.getExportConfig();
	}

	@Override
	protected IInputStream getInputStream() {
		return new SpaceInputStream(destination);
	}

	@Override
	protected IOutputStream getOutputStream() {
		return destination.getOutputStream();
	}

}
