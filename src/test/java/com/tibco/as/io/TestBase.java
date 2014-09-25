package com.tibco.as.io;

import org.junit.After;
import org.junit.Before;

import com.tibco.as.space.ASException;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;

public class TestBase {

	private Metaspace metaspace;

	@Before
	public void connectMetaspace() throws ASException {
		MemberDef memberDef = MemberDef.create(null, "tcp", null);
		memberDef.setConnectTimeout(10000);
		metaspace = Metaspace.connect(null, memberDef);
	}

	protected Metaspace getMetaspace() {
		return metaspace;
	}

	@After
	public void closeMetaspace() throws ASException {
		metaspace.closeAll();
	}

}
