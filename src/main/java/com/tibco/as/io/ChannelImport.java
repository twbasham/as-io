package com.tibco.as.io;

import java.util.Collection;

public class ChannelImport extends AbstractChannelTransfer {

	private Channel channel;

	public ChannelImport(Channel channel) {
		this.channel = channel;
	}

	@Override
	protected DestinationImport getTransfer(Destination destination) {
		return new DestinationImport(destination);
	}

	@Override
	protected Collection<Destination> getDestinations() throws Exception {
		return channel.getImportDestinations();
	}

}
