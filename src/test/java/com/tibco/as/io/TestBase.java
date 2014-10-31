package com.tibco.as.io;

import org.junit.After;
import org.junit.Before;

import com.tibco.as.util.Member;

public class TestBase {

	private static final String DISCOVERY = "tcp";

	private TestChannel channel;

	protected AbstractChannel getChannel() {
		return channel;
	}

	@Before
	public void openChannel() throws Exception {
		channel = new TestChannel();
		Member member = new Member();
		member.setDiscovery(DISCOVERY);
		member.setConnectTimeout(10000L);
		channel.setMember(member);
		channel.open();
	}

	@After
	public void closeChannel() throws Exception {
		channel.close();
	}

}
