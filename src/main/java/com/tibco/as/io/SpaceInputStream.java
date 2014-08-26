package com.tibco.as.io;

import java.text.MessageFormat;

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

public class SpaceInputStream implements IInputStream<Tuple> {

	private Metaspace metaspace;

	private String spaceName;

	private Export export;

	private Browser browser;

	private long position;

	private long browseTime;

	private long size = IInputStream.UNKNOWN_SIZE;

	public SpaceInputStream(Metaspace metaspace, String spaceName, Export export) {
		this.metaspace = metaspace;
		this.spaceName = spaceName;
		this.export = export;
	}

	@Override
	public long getOpenTime() {
		return browseTime;
	}

	@Override
	public void open() throws ASException {
		BrowserDef browserDef = BrowserDef.create();
		if (export.getTimeScope() != null) {
			browserDef.setTimeScope(export.getTimeScope());
		}
		if (export.getDistributionScope() != null) {
			browserDef.setDistributionScope(export.getDistributionScope());
		}
		if (export.getTimeout() != null) {
			browserDef.setTimeout(export.getTimeout());
		}
		if (export.getPrefetch() != null) {
			browserDef.setPrefetch(export.getPrefetch());
		}
		if (export.getQueryLimit() != null) {
			if (Utils.hasMethod(BrowserDef.class, "setQueryLimit")) {
				browserDef.setQueryLimit(export.getQueryLimit());
			}
		}
		String filter = export.getFilter();
		long start = System.nanoTime();
		if (filter == null) {
			browser = metaspace.browse(spaceName, BrowserType.GET, browserDef);
		} else {
			browser = metaspace.browse(spaceName, BrowserType.GET, browserDef,
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
		try {
			Tuple tuple = browser.next();
			position++;
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

	@Override
	public String getName() {
		return MessageFormat.format("space ''{0}''", spaceName);
	}

}
