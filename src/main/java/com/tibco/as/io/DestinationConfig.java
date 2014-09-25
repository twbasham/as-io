package com.tibco.as.io;

import com.tibco.as.convert.Attributes;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class DestinationConfig implements Cloneable {

	private Direction direction;

	private Integer batchSize;

	private Integer workerCount;

	private Integer queueCapacity;

	private Long limit;

	private String spaceName;

	private Attributes attributes = new Attributes();

	private DistributionRole distributionRole;

	private Boolean keepSpaceOpen;

	private OperationType operation;

	private Long waitForReadyTimeout;

	private BrowserType browserType;

	private TimeScope timeScope;

	private DistributionScope distributionScope;

	private Long timeout;

	private Long prefetch;

	private Long queryLimit;

	private String filter;

	public Direction getDirection() {
		return direction;
	}

	public void setDirection(Direction direction) {
		this.direction = direction;
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

	public boolean isAllOrNew() {
		return TimeScope.ALL == timeScope || TimeScope.NEW == timeScope;
	}

	public DistributionRole getDistributionRole() {
		return distributionRole;
	}

	public void setDistributionRole(DistributionRole distributionRole) {
		this.distributionRole = distributionRole;
	}

	public Long getWaitForReadyTimeout() {
		return waitForReadyTimeout;
	}

	public void setWaitForReadyTimeout(Long waitForReadyTimeout) {
		this.waitForReadyTimeout = waitForReadyTimeout;
	}

	public Boolean isKeepSpaceOpen() {
		return keepSpaceOpen;
	}

	public void setKeepSpaceOpen(Boolean keepSpaceOpen) {
		this.keepSpaceOpen = keepSpaceOpen;
	}

	public OperationType getOperation() {
		return operation;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public String getSpaceName() {
		return spaceName;
	}

	public void setSpaceName(String spaceName) {
		this.spaceName = spaceName;
	}

	public Attributes getAttributes() {
		return attributes;
	}

	public void setAttributes(Attributes attributes) {
		this.attributes = attributes;
	}

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public Integer getWorkerCount() {
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
		target.batchSize = batchSize;
		target.queueCapacity = queueCapacity;
		target.workerCount = workerCount;
		target.limit = limit;
		target.spaceName = spaceName;
		target.attributes = attributes;
		target.distributionRole = distributionRole;
		target.keepSpaceOpen = keepSpaceOpen;
		target.operation = operation;
		target.waitForReadyTimeout = waitForReadyTimeout;
		target.browserType = browserType;
		target.distributionScope = distributionScope;
		target.filter = filter;
		target.prefetch = prefetch;
		target.queryLimit = queryLimit;
		target.timeout = timeout;
		target.timeScope = timeScope;
	}

}
