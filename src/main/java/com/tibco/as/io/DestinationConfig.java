package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.tibco.as.convert.ConversionConfig;
import com.tibco.as.convert.Field;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class DestinationConfig implements Cloneable {

	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final int DEFAULT_BATCH_SIZE_CONTINUOUS = 1;
	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;
	private static final int DEFAULT_WORKER_COUNT = 1;

	private String space;
	private Collection<Field> fields = new ArrayList<Field>();
	private Collection<String> keys = new ArrayList<String>();
	private ConversionConfig conversion = new ConversionConfig();
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
	private Direction direction;
	private boolean noTransfer;

	@Override
	public DestinationConfig clone() {
		DestinationConfig clone = new DestinationConfig();
		copyTo(clone);
		return clone;
	}

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

	public void copyTo(DestinationConfig target) {
		for (Field field : fields) {
			target.fields.add(field.clone());
		}
		target.keys = new ArrayList<String>(keys);
		target.space = space;
		target.browserType = browserType;
		target.direction = direction;
		target.distributionRole = distributionRole;
		target.distributionScope = distributionScope;
		target.filter = filter;
		target.limit = limit;
		target.noTransfer = noTransfer;
		target.operation = operation;
		target.prefetch = prefetch;
		target.queryLimit = queryLimit;
		target.queueCapacity = queueCapacity;
		target.spaceBatchSize = spaceBatchSize;
		target.timeout = timeout;
		target.timeScope = timeScope;
		target.waitForReadyTimeout = waitForReadyTimeout;
		target.workerCount = workerCount;
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

	public boolean isWildcard() {
		if (isImport()) {
			return false;
		}
		return getSpace() == null;
	}

	public Direction getDirection() {
		return direction;
	}

	public void setDirection(Direction direction) {
		this.direction = direction;
	}

	public boolean isImport() {
		return direction == Direction.IMPORT;
	}

	public boolean isNoTransfer() {
		return noTransfer;
	}

	public void setNoTransfer(boolean noTransfer) {
		this.noTransfer = noTransfer;
	}

	public void setFields(Collection<Field> fields) {
		this.fields = fields;
	}

	public Collection<Field> getFields() {
		return Collections.unmodifiableCollection(fields);
	}

	public Field getField(String name) {
		for (Field field : fields) {
			if (field.getFieldName().equals(name)) {
				return field;
			}
		}
		Field field = addField();
		field.setFieldName(name);
		return field;
	}

	public Collection<String> getKeys() {
		return keys;
	}

	public void setKeys(Collection<String> keys) {
		this.keys = keys;
	}

	public String getSpace() {
		return space;
	}

	public void setSpace(String space) {
		this.space = space;
	}

	public Field addField() {
		Field field = newField();
		fields.add(field);
		return field;
	}

	protected Field newField() {
		return new Field();
	}

	public Collection<String> getFieldNames() {
		Collection<String> fieldNames = new ArrayList<String>();
		for (Field field : getFields()) {
			fieldNames.add(field.getFieldName());
		}
		return fieldNames;
	}

	public void setFieldNames(List<String> fieldNames) {
		Collection<Field> fields = new ArrayList<Field>();
		for (String fieldName : fieldNames) {
			fields.add(getField(fieldName));
		}
		setFields(fields);
	}

	public void setSpaceDef(SpaceDef spaceDef) {
		setSpace(spaceDef.getName());
		setKeys(spaceDef.getKeyDef().getFieldNames());
		if (getFields().isEmpty()) {
			for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
				addField().setFieldName(fieldDef.getName());
			}
		}
		Collection<Field> fields = new ArrayList<Field>();
		for (Field field : getFields()) {
			FieldDef fieldDef = spaceDef.getFieldDef(field.getFieldName());
			if (fieldDef == null) {
				field.setFieldName(null);
				field.setFieldType(null);
			} else {
				field.setFieldType(fieldDef.getType());
				field.setFieldNullable(fieldDef.isNullable());
				field.setFieldEncrypted(fieldDef.isEncrypted());
			}
			fields.add(field);
		}
		setFields(fields);
		setKeys(spaceDef.getKeyDef().getFieldNames());
	}

	public ConversionConfig getConversion() {
		return conversion;
	}

	public SpaceDef getSpaceDef() {
		SpaceDef spaceDef = SpaceDef.create(getSpace());
		for (Field field : getFields()) {
			FieldType fieldType = field.getFieldType();
			if (fieldType == null) {
				fieldType = field.getJavaFieldType();
			}
			FieldDef fieldDef = FieldDef
					.create(field.getFieldName(), fieldType);
			Boolean encrypted = field.getFieldEncrypted();
			if (encrypted != null) {
				fieldDef.setEncrypted(encrypted);
			}
			Boolean nullable = field.getFieldNullable();
			if (nullable != null) {
				fieldDef.setNullable(nullable);
			}
			spaceDef.getFieldDefs().add(fieldDef);
		}
		Collection<String> keys = getKeys();
		if (keys.isEmpty()) {
			keys = getFieldNames();
		}
		spaceDef.setKey(keys.toArray(new String[keys.size()]));
		return spaceDef;
	}

}
