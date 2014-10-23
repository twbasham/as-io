package com.tibco.as.io;

import com.tibco.as.space.ASException;
import com.tibco.as.space.ASStatus;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.RuntimeASException;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.impl.ASBrowser;
import com.tibco.as.util.Utils;

public class SpaceInputStream implements IInputStream {

	private Metaspace metaspace;
	private DestinationConfig config;
	private Browser browser;
	private long position;
	private long browseTime;
	private Long size;

	public SpaceInputStream(Metaspace metaspace, DestinationConfig config) {
		this.metaspace = metaspace;
		this.config = config;
	}

	@Override
	public long getOpenTime() {
		return browseTime;
	}

	@Override
	public void open() throws Exception {
		config.setSpaceDef(metaspace.getSpaceDef(config.getSpace()));
		BrowserType browserType = config.getBrowserType();
		if (browserType == null) {
			browserType = BrowserType.GET;
		}
		BrowserDef browserDef = BrowserDef.create();
		if (browserType == BrowserType.GET) {
			if (config.getTimeScope() != null) {
				browserDef.setTimeScope(config.getTimeScope());
			}
		}
		if (config.getDistributionScope() != null) {
			browserDef.setDistributionScope(config.getDistributionScope());
		}
		if (config.getTimeout() != null) {
			browserDef.setTimeout(config.getTimeout());
		}
		if (config.getPrefetch() != null) {
			browserDef.setPrefetch(config.getPrefetch());
		}
		if (config.getQueryLimit() != null) {
			if (Utils.hasMethod(BrowserDef.class, "setQueryLimit")) {
				browserDef.setQueryLimit(config.getQueryLimit());
			}
		}
		String filter = config.getFilter();
		String space = config.getSpace();
		long start = System.nanoTime();
		if (filter == null) {
			browser = metaspace.browse(space, browserType, browserDef);
		} else {
			browser = metaspace.browse(space, browserType, browserDef, filter);
		}
		browseTime = System.nanoTime() - start;
		if (browser instanceof ASBrowser) {
			size = ((ASBrowser) browser).size();
		}
		position = 0;
	}

	@Override
	public Tuple read() throws ASException {
		if (browser == null) {
			return null;
		}
		try {
			Tuple tuple = browser.next();
			if (tuple != null) {
				position++;
			}
			return tuple;
		} catch (RuntimeASException e) {
			if (e.getCause() instanceof ASException) {
				ASException ase = (ASException) e.getCause();
				if (ase.getStatus() == ASStatus.INVALID_OBJECT) {
					return null;
				}
				throw ase;
			}
			if (e.getStatus() == ASStatus.INVALID_OBJECT) {
				return null;
			}
			throw e;
		} catch (ASException e) {
			if (e.getStatus() == ASStatus.INVALID_OBJECT) {
				return null;
			}
			throw e;
		}
	}

	@Override
	public Long getPosition() {
		return position;
	}

	@Override
	public Long size() {
		return size;
	}

	@Override
	public void close() throws ASException {
		if (browser == null) {
			return;
		}
		browser.stop();
		browser = null;
	}
}
