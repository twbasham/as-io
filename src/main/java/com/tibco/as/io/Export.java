package com.tibco.as.io;

public class Export extends AbstractTransfer {

	public Export(Destination destination) {
		super(destination);
	}

	@Override
	protected IInputStream getInputStream(Destination destination) {
		return new SpaceInputStream(destination);
	}

	@Override
	protected IOutputStream getOutputStream(Destination destination) {
		return destination.getOutputStream();
	}

	@Override
	protected int getWorkerCount(Destination destination) {
		return destination.getExportWorkerCount();
	}

}
