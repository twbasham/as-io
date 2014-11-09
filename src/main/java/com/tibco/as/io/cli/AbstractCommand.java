package com.tibco.as.io.cli;

import com.tibco.as.io.IChannel;
import com.tibco.as.io.IChannelTransfer;
import com.tibco.as.io.IDestination;

public abstract class AbstractCommand implements ICommand {

	@Override
	public IChannelTransfer getTransfer(IChannel channel) {
		configure(channel.getDefaultDestination());
		addDestinations(channel);
		return createTransfer(channel);
	}

	protected abstract void addDestinations(IChannel channel);

	protected abstract void configure(IDestination destination);

	protected abstract IChannelTransfer createTransfer(IChannel channel);

}
