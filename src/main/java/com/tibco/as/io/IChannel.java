package com.tibco.as.io;

import java.util.Collection;

public interface IChannel {

	void start() throws Exception;

	void awaitTermination();

	void stop() throws Exception;

	void addListener(IChannelListener listener);

	Collection<IDestination> getDestinations();

}
