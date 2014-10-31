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

public class Destination {

	private static final int DEFAULT_WORKER_COUNT = 1;
	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;

	private ConverterFactory converterFactory = new ConverterFactory();
	private AbstractChannel channel;
	private String space;
	private Collection<Field> fields;
	private Collection<String> keys;
	private Settings settings;
	private Integer importWorkerCount;
	private Long importLimit;
	private Integer exportWorkerCount;
	private Long exportLimit;
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

	public Destination(AbstractChannel channel) {
		this.channel = channel;
	}

	public void copyTo(Destination target) {
		if (target.space == null) {
			target.space = space;
		}
		if (target.fields == null) {
			target.fields = new ArrayList<Field>();
			for (Field field : getFields()) {
				target.fields.add(field.clone());
			}
		}
		if (target.keys == null) {
			if (keys != null) {
				target.keys = new ArrayList<String>(keys);
			}
		}
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
		if (target.exportLimit == null) {
			target.exportLimit = exportLimit;
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

	public int getExportWorkerCount() {
		if (exportWorkerCount == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return exportWorkerCount;
	}

	public void setExportWorkerCount(Integer workerCount) {
		this.exportWorkerCount = workerCount;
	}

	public Long getExportLimit() {
		return exportLimit;
	}

	public void setExportLimit(Long exportLimit) {
		this.exportLimit = exportLimit;
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
		if (fields == null) {
			fields = new ArrayList<Field>();
		}
		return fields;
	}

	public void setFields(Collection<Field> fields) {
		this.fields = fields;
	}

	public Field getField(String name) {
		for (Field field : getFields()) {
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
		getFields().add(field);
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
		if (fieldNames == null) {
			return;
		}
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

	public IInputStream getInputStream() {
		return null;
	}

	public IOutputStream getOutputStream() {
		return null;
	}

	public String getName() {
		return getSpace();
	}

	public Export getExport() throws Exception {
		return new Export(this);
	}

	public Import getImport() throws Exception {
		return new Import(this);
	}

	public AbstractChannel getChannel() {
		return channel;
	}

	public IAccessor[] getObjectAccessors() {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		int size = getFields().size();
		for (int index = 0; index < size; index++) {
			accessors.add(new ObjectArrayAccessor(index));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public IAccessor[] getTupleAccessors() {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		for (Field field : getFields()) {
			String fieldName = field.getFieldName();
			FieldType fieldType = field.getFieldType();
			accessors.add(converterFactory.getAccessor(fieldName, fieldType));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public IConverter[] getJavaConverters() {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (Field field : getFields()) {
			Class<?> javaType = field.getJavaType();
			FieldType fieldType = field.getFieldType();
			converters.add(converterFactory.getConverter(settings, javaType,
					fieldType));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	public IConverter[] getFieldConverters() {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (Field field : getFields()) {
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