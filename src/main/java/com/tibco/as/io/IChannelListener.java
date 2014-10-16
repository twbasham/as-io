package com.tibco.as.io;

public interface IChannelListener {

	void starting(IDestination destination);

	void started(IDestination destination);

	void stopping(IDestination destination);

	void stopped(IDestination destination);

}
