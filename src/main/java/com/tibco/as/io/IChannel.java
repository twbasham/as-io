package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Member;
import com.tibco.as.util.convert.Settings;

public interface IChannel {

	String getMetaspaceName();

	void setMetaspaceName(String metaspaceName);

	Member getMember();

	Settings getSettings();

	void open() throws Exception;

	void close() throws Exception;

	Collection<IDestination> getDestinations();

	Metaspace getMetaspace();

}
