package com.tibco.as.io;

import java.util.Collection;

public class ChannelExport extends AbstractChannelTransfer {

	public ChannelExport(Channel channel) {
		super(channel);
	}

	@Override
	protected DestinationExport getTransfer(Destination destination) throws Exception {
		return destination.getExport();
	}

	@Override
	protected Collection<Destination> getDestinations(Channel channel)
			throws Exception {
		return channel.getExportDestinations();
	}

}
