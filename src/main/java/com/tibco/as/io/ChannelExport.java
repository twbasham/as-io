package com.tibco.as.io;

import java.util.Collection;

public class ChannelExport extends AbstractChannelTransfer {

	private Channel channel;

	public ChannelExport(Channel channel) {
		this.channel = channel;
	}

	@Override
	protected DestinationExport getTransfer(Destination destination) {
		return new DestinationExport(destination);
	}

	@Override
	protected Collection<Destination> getDestinations() throws Exception {
		return channel.getExportDestinations();
	}

}
