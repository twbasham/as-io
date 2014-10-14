package com.tibco.as.io;

import com.tibco.as.convert.Space;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class DestinationConfig extends Space {

	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final int DEFAULT_BATCH_SIZE_CONTINUOUS = 1;
	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;
	private static final int DEFAULT_WORKER_COUNT = 1;

	private Integer spaceBatchSize;
	private Integer workerCount;
	private Integer queueCapacity;
	private Long limit;
	private OperationType operation;
	private Long waitForReadyTimeout;
	private DistributionRole distributionRole;
	private BrowserType browserType;
	private TimeScope timeScope;
	private DistributionScope distributionScope;
	private Long timeout;
	private Long prefetch;
	private Long queryLimit;
	private String filter;

	public Integer getSpaceBatchSize() {
		if (spaceBatchSize == null) {
			TimeScope timeScope = getTimeScope();
			if (timeScope == TimeScope.ALL || timeScope == TimeScope.NEW) {
				return DEFAULT_BATCH_SIZE_CONTINUOUS;
			}
			return DEFAULT_BATCH_SIZE;
		}
		return spaceBatchSize;
	}

	public void setSpaceBatchSize(Integer spaceBatchSize) {
		this.spaceBatchSize = spaceBatchSize;
	}

	public int getWorkerCount() {
		if (workerCount == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return workerCount;
	}

	public void setWorkerCount(Integer workerCount) {
		this.workerCount = workerCount;
	}

	public Integer getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(Integer queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	@Override
	public DestinationConfig clone() {
		DestinationConfig clone = new DestinationConfig();
		copyTo(clone);
		return clone;
	}

	public void copyTo(DestinationConfig target) {
		target.browserType = browserType;
		target.distributionRole = distributionRole;
		target.distributionScope = distributionScope;
		target.filter = filter;
		target.limit = limit;
		target.operation = operation;
		target.prefetch = prefetch;
		target.queryLimit = queryLimit;
		target.queueCapacity = queueCapacity;
		target.spaceBatchSize = spaceBatchSize;
		target.timeout = timeout;
		target.timeScope = timeScope;
		target.waitForReadyTimeout = waitForReadyTimeout;
		target.workerCount = workerCount;
		super.copyTo(target);
	}

	public BrowserType getBrowserType() {
		return browserType;
	}

	public void setBrowserType(BrowserType browserType) {
		this.browserType = browserType;
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

	public DistributionRole getDistributionRole() {
		return distributionRole;
	}

	public void setDistributionRole(DistributionRole distributionRole) {
		this.distributionRole = distributionRole;
	}

	public long getWaitForReadyTimeout() {
		if (waitForReadyTimeout == null) {
			return DEFAULT_WAIT_FOR_READY_TIMEOUT;
		}
		return waitForReadyTimeout;
	}

	public void setWaitForReadyTimeout(Long waitForReadyTimeout) {
		this.waitForReadyTimeout = waitForReadyTimeout;
	}

	public OperationType getOperation() {
		return operation;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

}
