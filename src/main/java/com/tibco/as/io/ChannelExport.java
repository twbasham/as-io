package com.tibco.as.io;

import java.util.Collection;

public class ChannelExport extends AbstractChannelTransfer {

	public ChannelExport(Channel channel) {
		super(channel);
	}

	@Override
	protected DestinationTransfer getTransfer(Destination destination)
			throws Exception {
		String name = destination.getName();
		int workerCount = destination.getExportWorkerCount();
		IInputStream in = new SpaceInputStream(destination);
		IOutputStream out = destination.getOutputStream();
		return new DestinationTransfer(name, workerCount, in, out);
	}

	@Override
	protected Collection<Destination> getDestinations(Channel channel)
			throws Exception {
		return channel.getExportDestinations();
	}

}
