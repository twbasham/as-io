package com.tibco.as.io.cli;

import com.tibco.as.io.Channel;
import com.tibco.as.io.IChannelTransfer;

public interface ICommand {

	IChannelTransfer getTransfer(Channel channel) throws Exception;

}