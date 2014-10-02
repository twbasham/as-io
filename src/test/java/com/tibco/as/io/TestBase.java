package com.tibco.as.io;

import org.junit.After;
import org.junit.Before;

import com.tibco.as.space.ASException;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;

public class TestBase {

	private static final String DISCOVERY = "tcp";

	private Metaspace metaspace;

	protected ChannelConfig getChannelConfig() {
		ChannelConfig config = new ChannelConfig();
		config.setDiscovery(DISCOVERY);
		config.setConnectTimeout(10000L);
		return config;
	}

	@Before
	public void connectMetaspace() throws ASException {
		metaspace = Metaspace.connect(null,
				MemberDef.create(null, DISCOVERY, null));
	}

	protected Metaspace getMetaspace() throws ASException {
		return metaspace;
	}

	@After
	public void closeMetaspace() throws ASException {
		metaspace.closeAll();
	}

}
