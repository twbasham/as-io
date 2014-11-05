package com.tibco.as.io;

import java.util.Collection;

public class ChannelExport extends AbstractChannelTransfer {

	public ChannelExport(Channel channel) {
		super(channel);
	}

	@Override
	protected DestinationExport getTransfer(Destination destination) {
		return new DestinationExport(destination);
	}

	@Override
	protected Collection<Destination> getDestinations(Channel channel)
			throws Exception {
		return channel.getExportDestinations();
	}

}
