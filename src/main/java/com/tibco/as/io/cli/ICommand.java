package com.tibco.as.io.cli;

import com.tibco.as.io.IChannel;
import com.tibco.as.io.IChannelTransfer;

public interface ICommand {

	IChannelTransfer getTransfer(IChannel channel);

}