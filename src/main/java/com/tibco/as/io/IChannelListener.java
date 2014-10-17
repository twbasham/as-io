package com.tibco.as.io;

public interface IChannelListener {

	void started(IDestination destination);

	void completed(IDestination destination);

	void stopped(IDestination destination);

}
