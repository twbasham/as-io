package com.tibco.as.io;

import java.util.Collection;

public class ChannelImport extends AbstractChannelTransfer {

	public ChannelImport(Channel channel) {
		super(channel);
	}

	@Override
	protected DestinationImport getTransfer(Destination destination) throws Exception {
		return destination.getImport();
	}

	@Override
	protected Collection<Destination> getDestinations(Channel channel)
			throws Exception {
		return channel.getImportDestinations();
	}

}
