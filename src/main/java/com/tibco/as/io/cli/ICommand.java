package com.tibco.as.io.cli;

import com.tibco.as.io.ChannelConfig;

public interface ICommand {

	void configure(ChannelConfig config) throws Exception;

}
