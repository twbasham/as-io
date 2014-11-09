package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.util.Member;

public interface IChannel {

	Member getMember();

	IDestination getDefaultDestination();

	void open() throws Exception;

	ChannelImport getImport();

	ChannelExport getExport();

	void close() throws Exception;

	void setSpaceNames(Collection<String> spaceNames);

}
