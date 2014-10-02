package com.tibco.as.io;

public interface IChannelListener {

	void opening(IDestination destination);

	void opened(IDestination destination);

	void closing(IDestination destination);

	void closed(IDestination destination);

}
