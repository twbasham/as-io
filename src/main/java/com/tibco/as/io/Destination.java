package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IAccessor;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.Settings;
import com.tibco.as.convert.accessors.ObjectArrayAccessor;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;

public class Destination implements IDestination {

	private ConverterFactory converterFactory = new ConverterFactory();
	private Channel channel;
	private String spaceName;
	private Collection<FieldDef> fieldDefs = new ArrayList<FieldDef>();
	private Collection<String> keys = new ArrayList<String>();
	private Collection<String> fieldNames = new ArrayList<String>();
	private Settings settings = new Settings();
	private ExportConfig exportConfig = new ExportConfig();
	private ImportConfig importConfig = new ImportConfig();

	public Destination(Channel channel) {
		this.channel = channel;
	}

	public void copyTo(Destination target) {
		exportConfig.copyTo(target.exportConfig);
		importConfig.copyTo(target.importConfig);
		settings.copyTo(target.settings);
		if (target.spaceName == null) {
			target.spaceName = spaceName;
		}
		target.fieldDefs.addAll(fieldDefs);
		target.keys.addAll(keys);
		target.fieldNames = new ArrayList<String>(fieldNames);
	}

	@Override
	public ExportConfig getExportConfig() {
		return exportConfig;
	}

	@Override
	public ImportConfig getImportConfig() {
		return importConfig;
	}

	@Override
	public Settings getSettings() {
		return settings;
	}

	public IInputStream getInputStream() {
		return null;
	}

	public IOutputStream getOutputStream() {
		return null;
	}

	public String getName() {
		return spaceName;
	}

	public Channel getChannel() {
		return channel;
	}

	public IAccessor[] getObjectAccessors() {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		for (int index = 0; index < getFieldDefs().size(); index++) {
			accessors.add(new ObjectArrayAccessor(index));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public IAccessor[] getTupleAccessors() {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		for (FieldDef fieldDef : getFieldDefs()) {
			String fieldName = fieldDef.getName();
			FieldType fieldType = fieldDef.getType();
			accessors.add(converterFactory.getAccessor(fieldName, fieldType));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public Collection<String> getFieldNames() {
		Collection<String> fieldNames = new ArrayList<String>();
		for (FieldDef fieldDef : getFieldDefs()) {
			fieldNames.add(fieldDef.getName());
		}
		return fieldNames;
	}

	@Override
	public void setFieldNames(Collection<String> fieldNames) {
		this.fieldNames = fieldNames;
	}

	protected Collection<FieldDef> getFieldDefs() {
		if (fieldNames.isEmpty()) {
			return fieldDefs;
		}
		Collection<FieldDef> fieldDefs = new ArrayList<FieldDef>();
		for (String fieldName : fieldNames) {
			FieldDef fieldDef = getFieldDef(fieldName);
			if (fieldDef == null) {
				continue;
			}
			fieldDefs.add(fieldDef);
		}
		return fieldDefs;
	}

	protected FieldDef getFieldDef(String fieldName) {
		for (FieldDef fieldDef : fieldDefs) {
			if (fieldName.equals(fieldDef.getName())) {
				return fieldDef;
			}
		}
		return null;
	}

	public IConverter[] getJavaConverters() {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (FieldDef fieldDef : getFieldDefs()) {
			converters.add(converterFactory.getConverter(settings,
					getJavaType(fieldDef), fieldDef.getType()));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	protected Class<?> getJavaType(FieldDef fieldDef) {
		return converterFactory.getJavaType(fieldDef.getType());
	}

	public IConverter[] getFieldConverters() {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (FieldDef field : getFieldDefs()) {
			converters.add(converterFactory.getConverter(settings,
					field.getType(), getJavaType(field)));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	public Metaspace getMetaspace() {
		return channel.getMetaspace();
	}

	public String getSpaceName() {
		return spaceName;
	}

	public void setSpaceName(String spaceName) {
		this.spaceName = spaceName;
	}

	public void setSpaceDef(SpaceDef spaceDef) {
		this.spaceName = spaceDef.getName();
		this.fieldDefs = spaceDef.getFieldDefs();
		this.keys = spaceDef.getKeyDef().getFieldNames();
	}

	public Collection<String> getKeys() {
		return keys;
	}

	public SpaceDef getSpaceDef() {
		SpaceDef spaceDef = SpaceDef.create(getSpaceName());
		spaceDef.getFieldDefs().addAll(getFieldDefs());
		spaceDef.getKeyDef().setFieldNames(getKeys().toArray(new String[0]));
		return spaceDef;
	}

}