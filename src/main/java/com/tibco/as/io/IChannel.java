package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.space.Metaspace;

public interface IChannel {

	Metaspace getMetaspace();

	void open() throws Exception;

	void close() throws Exception;

	void addConfig(DestinationConfig config);

	Collection<DestinationConfig> getConfigs();

}
