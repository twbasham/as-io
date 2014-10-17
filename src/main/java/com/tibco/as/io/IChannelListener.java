package com.tibco.as.io;

public interface IChannelListener {

	void started(IDestination destination);

	void stopped(IDestination destination);

}
