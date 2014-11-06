package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IAccessor;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.Settings;
import com.tibco.as.convert.accessors.ObjectArrayAccessor;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class Destination implements Cloneable {

	private static final int DEFAULT_WORKER_COUNT = 1;
	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;

	private ConverterFactory converterFactory = new ConverterFactory();
	private Channel channel;
	private String space;
	private Collection<Field> fields = new ArrayList<Field>();
	private Collection<String> keys = new ArrayList<String>();
	private Settings settings;
	private Integer importWorkerCount;
	private Long importLimit;
	private Integer exportWorkerCount;
	private Long spaceLimit;
	private Integer spaceBatchSize;
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

	public Destination(Channel channel) {
		this.channel = channel;
	}

	@Override
	public Destination clone() {
		Destination destination = new Destination(channel);
		copyTo(destination);
		return destination;
	}

	public void copyTo(Destination target) {
		if (target.space == null) {
			target.space = space;
		}
		for (Field field : fields) {
			Field newField = newField();
			field.copyTo(newField);
			target.fields.add(newField);
		}
		target.keys = new ArrayList<String>(keys);
		if (target.settings == null) {
			if (settings != null) {
				target.settings = settings.clone();
			}
		}
		if (target.importLimit == null) {
			target.importLimit = importLimit;
		}
		if (target.importWorkerCount == null) {
			target.importWorkerCount = importWorkerCount;
		}
		if (target.spaceLimit == null) {
			target.spaceLimit = spaceLimit;
		}
		if (target.exportWorkerCount == null) {
			target.exportWorkerCount = exportWorkerCount;
		}
		if (target.distributionRole == null) {
			target.distributionRole = distributionRole;
		}
		if (target.operation == null) {
			target.operation = operation;
		}
		if (target.spaceBatchSize == null) {
			target.spaceBatchSize = spaceBatchSize;
		}
		if (target.waitForReadyTimeout == null) {
			target.waitForReadyTimeout = waitForReadyTimeout;
		}
		if (target.browserType == null) {
			target.browserType = browserType;
		}
		if (target.distributionScope == null) {
			target.distributionScope = distributionScope;
		}
		if (target.filter == null) {
			target.filter = filter;
		}
		if (target.prefetch == null) {
			target.prefetch = prefetch;
		}
		if (target.queryLimit == null) {
			target.queryLimit = queryLimit;
		}
		if (target.timeout == null) {
			target.timeout = timeout;
		}
		if (target.timeScope == null) {
			target.timeScope = timeScope;
		}
	}

	public int getImportWorkerCount() {
		if (importWorkerCount == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return importWorkerCount;
	}

	public void setImportWorkerCount(Integer workerCount) {
		this.importWorkerCount = workerCount;
	}

	public Long getImportLimit() {
		return importLimit;
	}

	public void setImportLimit(Long importLimit) {
		this.importLimit = importLimit;
	}

	public int getSpaceWorkerCount() {
		if (exportWorkerCount == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return exportWorkerCount;
	}

	public void setExportWorkerCount(Integer workerCount) {
		this.exportWorkerCount = workerCount;
	}

	public Long getSpaceLimit() {
		return spaceLimit;
	}

	public void setSpaceLimit(Long spaceLimit) {
		this.spaceLimit = spaceLimit;
	}

	public Integer getSpaceBatchSize() {
		return spaceBatchSize;
	}

	public void setSpaceBatchSize(Integer spaceBatchSize) {
		this.spaceBatchSize = spaceBatchSize;
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

	public Collection<Field> getFields() {
		return fields;
	}

	public void setFields(Collection<Field> fields) {
		this.fields = fields;
	}

	protected Field getField(String name) {
		for (Field field : fields) {
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

	public String getSpace() {
		return space;
	}

	public void setSpace(String space) {
		this.space = space;
	}

	protected Field newField() {
		return new Field();
	}

	public Collection<String> getFieldNames() {
		Collection<String> fieldNames = new ArrayList<String>();
		for (Field field : fields) {
			fieldNames.add(field.getFieldName());
		}
		return fieldNames;
	}

	public void setFieldNames(List<String> fieldNames) {
		for (String fieldName : fieldNames) {
			Field field = getField(fieldName);
			if (field == null) {
				field = newField();
				field.setFieldName(fieldName);
				fields.add(field);
			}
		}
		Collection<Field> fields = new ArrayList<Field>();
		for (Field field : this.fields) {
			if (fieldNames.contains(field.getFieldName())) {
				fields.add(field);
			}
		}
		setFields(fields);
	}

	public void setSpaceDef(SpaceDef spaceDef) {
		setSpace(spaceDef.getName());
		setKeys(spaceDef.getKeyDef().getFieldNames());
		if (fields.isEmpty()) {
			for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
				Field field = newField();
				field.setFieldName(fieldDef.getName());
				fields.add(field);
			}
		}
		Collection<Field> fields = new ArrayList<Field>();
		for (Field field : this.fields) {
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

	public Settings getSettings() {
		return settings;
	}

	public void setSettings(Settings settings) {
		this.settings = settings;
	}

	public BrowserType getBrowserType() {
		if (browserType == null) {
			return BrowserType.GET;
		}
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

	public SpaceDef getSpaceDef() {
		SpaceDef spaceDef = SpaceDef.create(getSpace());
		for (Field field : fields) {
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

	public IInputStream getInputStream() throws Exception {
		return null;
	}

	public IOutputStream getOutputStream() {
		return null;
	}

	public String getName() {
		return getSpace();
	}

	public Channel getChannel() {
		return channel;
	}

	public IAccessor[] getObjectAccessors() {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		int size = fields.size();
		for (int index = 0; index < size; index++) {
			accessors.add(new ObjectArrayAccessor(index));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public IAccessor[] getTupleAccessors() {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		for (Field field : fields) {
			String fieldName = field.getFieldName();
			FieldType fieldType = field.getFieldType();
			accessors.add(converterFactory.getAccessor(fieldName, fieldType));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public IConverter[] getJavaConverters() {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (Field field : fields) {
			Class<?> javaType = field.getJavaType();
			FieldType fieldType = field.getFieldType();
			converters.add(converterFactory.getConverter(settings, javaType,
					fieldType));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	public IConverter[] getFieldConverters() {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (Field field : fields) {
			FieldType fieldType = field.getFieldType();
			Class<?> javaType = field.getJavaType();
			converters.add(converterFactory.getConverter(settings, fieldType,
					javaType));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	public Metaspace getMetaspace() {
		return channel.getMetaspace();
	}

}