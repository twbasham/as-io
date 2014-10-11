package com.tibco.as.io;

import java.text.MessageFormat;

import com.tibco.as.space.ASException;
import com.tibco.as.space.ASStatus;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.RuntimeASException;
import com.tibco.as.space.SpaceDef;
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

	private long size = IInputStream.UNKNOWN_SIZE;

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
		String spaceName = config.getSpace();
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		if (spaceDef == null) {
			throw new Exception(MessageFormat.format("No space named ''{0}''",
					spaceName));
		}
		config.setSpaceDef(spaceDef);
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
		long start = System.nanoTime();
		if (filter == null) {
			browser = metaspace.browse(spaceName, browserType, browserDef);
		} else {
			browser = metaspace.browse(spaceName, browserType, browserDef,
					filter);
		}
		browseTime = System.nanoTime() - start;
		if (browser instanceof ASBrowser) {
			size = ((ASBrowser) browser).size();
		}
		position = 0;
	}

	@Override
	public Tuple read() throws ASException {
		if (isClosed()) {
			return null;
		}
		try {
			return next();
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

	private Tuple next() throws ASException {
		Tuple tuple = browser.next();
		if (tuple == null) {
			return null;
		}
		position++;
		return tuple;
	}

	@Override
	public long getPosition() {
		return position;
	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public void close() throws ASException {
		if (isClosed()) {
			return;
		}
		browser.stop();
		browser = null;
	}

	@Override
	public boolean isClosed() {
		return browser == null;
	}

}
