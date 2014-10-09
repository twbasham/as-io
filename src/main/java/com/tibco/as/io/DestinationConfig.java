package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tibco.as.convert.Attributes;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class DestinationConfig implements Cloneable {

	private Direction direction;
	private Integer putBatchSize;
	private Integer workerCount;
	private Integer queueCapacity;
	private Long limit;
	private String space;
	private Attributes attributes = new Attributes();
	private DistributionRole distributionRole;
	private OperationType operation;
	private Long waitForReadyTimeout;
	private BrowserType browserType;
	private TimeScope timeScope;
	private DistributionScope distributionScope;
	private Long timeout;
	private Long prefetch;
	private Long queryLimit;
	private String filter;
	private List<FieldConfig> fields = new ArrayList<FieldConfig>();
	private Collection<String> keys;

	public List<FieldConfig> getFields() {
		return fields;
	}

	public void setFields(List<FieldConfig> fields) {
		this.fields = fields;
	}

	protected FieldConfig getField(String name) {
		for (FieldConfig field : fields) {
			if (field.getFieldName().equals(name)) {
				return field;
			}
		}
		return null;
	}

	public Collection<String> getKeys() {
		return keys;
	}

	public void setKeys(Collection<String> keys) {
		this.keys = keys;
	}

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

	public OperationType getOperation() {
		return operation;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public String getSpace() {
		return space;
	}

	public void setSpace(String space) {
		this.space = space;
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

	public Integer getPutBatchSize() {
		return putBatchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.putBatchSize = batchSize;
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
		target.attributes = attributes;
		target.browserType = browserType;
		target.direction = direction;
		target.distributionRole = distributionRole;
		target.distributionScope = distributionScope;
		target.filter = filter;
		target.limit = limit;
		target.operation = operation;
		target.prefetch = prefetch;
		target.putBatchSize = putBatchSize;
		target.queryLimit = queryLimit;
		target.queueCapacity = queueCapacity;
		target.space = space;
		target.timeout = timeout;
		target.timeScope = timeScope;
		target.waitForReadyTimeout = waitForReadyTimeout;
		target.workerCount = workerCount;
		target.fields = new ArrayList<FieldConfig>();
		for (FieldConfig field : fields) {
			target.fields.add(field.clone());
		}
		target.keys = keys == null ? null : new ArrayList<String>(keys);
	}

	public FieldConfig createFieldConfig() {
		return new FieldConfig();
	}

}
