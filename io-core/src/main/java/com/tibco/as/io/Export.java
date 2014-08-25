package com.tibco.as.io;

import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class Export extends Transfer implements Cloneable {

	private TimeScope timeScope;

	private DistributionScope distributionScope;

	private Long timeout;

	private Long prefetch;

	private Long queryLimit;

	private String filter;

	@Override
	public Export clone() {
		Export export = new Export();
		copyTo(export);
		return export;
	}

	public TimeScope getTimeScope() {
		return timeScope;
	}

	public void setTimeScope(TimeScope timeScope) {
		this.timeScope = timeScope;
	}

	public DistributionScope getDistributionScope() {
		return distributionScope;
	}

	public void setDistributionScope(DistributionScope distributionScope) {
		this.distributionScope = distributionScope;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public Long getTimeout() {
		return timeout;
	}

	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}

	public Long getPrefetch() {
		return prefetch;
	}

	public void setPrefetch(Long prefetch) {
		this.prefetch = prefetch;
	}

	public Long getQueryLimit() {
		return queryLimit;
	}

	public void setQueryLimit(Long queryLimit) {
		this.queryLimit = queryLimit;
	}

	public void copyTo(Export export) {
		export.distributionScope = distributionScope;
		export.filter = filter;
		export.prefetch = prefetch;
		export.queryLimit = queryLimit;
		export.timeout = timeout;
		export.timeScope = timeScope;
		super.copyTo(export);
	}

	public boolean isAllOrNew() {
		return TimeScope.ALL == timeScope || TimeScope.NEW == timeScope;
	}

}
