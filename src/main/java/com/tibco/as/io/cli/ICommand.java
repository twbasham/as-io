package com.tibco.as.io.cli;

import com.tibco.as.io.Channel;
import com.tibco.as.io.AbstractChannelTransfer;

public interface ICommand {

	AbstractChannelTransfer getTransfer(Channel channel) throws Exception;

}