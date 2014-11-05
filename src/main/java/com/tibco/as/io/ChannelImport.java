package com.tibco.as.io;

import java.util.Collection;

public class ChannelImport extends AbstractChannelTransfer {

	public ChannelImport(Channel channel) {
		super(channel);
	}

	@Override
	protected DestinationImport getTransfer(Destination destination) {
		return new DestinationImport(destination);
	}

	@Override
	protected Collection<Destination> getDestinations(Channel channel)
			throws Exception {
		return channel.getImportDestinations();
	}

}
