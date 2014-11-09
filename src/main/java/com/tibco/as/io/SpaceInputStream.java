package com.tibco.as.io;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
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

public class SpaceInputStream implements IInputStream {

	private Logger log = LogFactory.getLog(SpaceInputStream.class);
	private Destination destination;
	private Browser browser;
	private Long position;
	private long browseTime;
	private Long size;

	public SpaceInputStream(Destination destination) {
		this.destination = destination;
	}

	@Override
	public long getOpenTime() {
		return browseTime;
	}

	@Override
	public synchronized void open() throws Exception {
		if (browser != null) {
			return;
		}
		Metaspace metaspace = destination.getMetaspace();
		SpaceDef spaceDef = metaspace.getSpaceDef(getSpaceName());
		destination.setSpaceDef(spaceDef);
		BrowserDef browserDef = getBrowserDef();
		long start = System.nanoTime();
		browser = getBrowser(metaspace, browserDef);
		browseTime = System.nanoTime() - start;
		if (browser instanceof ASBrowser) {
			size = ((ASBrowser) browser).size();
		}
		position = 0L;
	}

	private BrowserDef getBrowserDef() {
		return destination.getExportConfig().getBrowserConfig().getBrowserDef();
	}

	private Browser getBrowser(Metaspace metaspace, BrowserDef browserDef)
			throws ASException {
		BrowserType browserType = getBrowserType();
		String filter = destination.getExportConfig().getBrowserConfig()
				.getFilter();
		String space = getSpaceName();
		if (filter == null) {
			log.log(Level.FINE,
					"Browsing space ''{0}'' with type {1} and def {2}",
					new Object[] { space, browserType, browserDef });
			return metaspace.browse(space, browserType, browserDef);
		}
		log.log(Level.FINE,
				"Browsing space ''{0}'' with type {1}, def {2} and filter {3}",
				new Object[] { space, browserType, browserDef, filter });
		return metaspace.browse(space, browserType, browserDef, filter);
	}

	private String getSpaceName() {
		return destination.getSpaceName();
	}

	private BrowserType getBrowserType() {
		BrowserType type = destination.getExportConfig().getBrowserConfig()
				.getBrowserType();
		if (type == null) {
			return BrowserType.GET;
		}
		return type;
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
	public synchronized void close() throws ASException {
		if (browser == null) {
			return;
		}
		log.fine("Stopping browser");
		browser.stop();
		browser = null;
	}
}
