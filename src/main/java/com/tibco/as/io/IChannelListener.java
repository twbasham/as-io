package com.tibco.as.io;

public interface IChannelListener {

	void opened(IDestination destination);

	void closed(IDestination destination);

}
