package com.tibco.as.io;

public interface IChannelTransfer {

	void addListener(IChannelTransferListener listener);

	void execute() throws Exception;

	void prepare() throws Exception;

}
