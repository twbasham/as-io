package com.tibco.as.io.cli;

import com.tibco.as.io.ChannelTransfer;
import com.tibco.as.io.IChannel;

public interface ICommand {

	ChannelTransfer getTransfer(IChannel channel) throws Exception;

}