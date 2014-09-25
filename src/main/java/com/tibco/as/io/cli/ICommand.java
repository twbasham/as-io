package com.tibco.as.io.cli;

import com.tibco.as.io.IChannel;

public interface ICommand {

	void configure(IChannel channel) throws Exception;

}
