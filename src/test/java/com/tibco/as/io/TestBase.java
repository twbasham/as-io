package com.tibco.as.io;

import org.junit.After;
import org.junit.Before;

import com.tibco.as.space.ASException;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Member;

public class TestBase {

	private static final String DISCOVERY = "tcp";

	private Metaspace metaspace;

	protected TestChannelConfig getChannelConfig() {
		TestChannelConfig config = new TestChannelConfig();
		Member member = new Member();
		member.setDiscovery(DISCOVERY);
		member.setConnectTimeout(10000L);
		config.setMember(member);
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
