package com.tibco.as.io;

import com.tibco.as.space.Metaspace;

public interface IDestination {

	void open(Metaspace metaspace) throws Exception;

	void close() throws Exception;

	void stop() throws Exception;

}