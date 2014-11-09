package com.tibco.as.io;

public class DestinationExport extends AbstractDestinationTransfer {

	private Destination destination;

	public DestinationExport(Destination destination) {
		super(destination);
		this.destination = destination;
	}

	@Override
	protected TransferConfig getConfig() {
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
