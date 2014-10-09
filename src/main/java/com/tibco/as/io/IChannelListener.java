package com.tibco.as.io;

public interface IChannelListener {

	void opening(IDestination destination);

	void opened(IDestination destination);

	void closing(IDestination destination);

	void closed(IDestination destination);

	void opening(ITransfer transfer);
	
	void opened(ITransfer transfer);
	
	void closing(ITransfer transfer);
	
	void closed(ITransfer transfer);

}
