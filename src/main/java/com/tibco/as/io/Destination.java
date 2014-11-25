package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.util.accessors.ObjectArrayAccessor;
import com.tibco.as.util.convert.ConverterFactory;
import com.tibco.as.util.convert.IAccessor;
import com.tibco.as.util.convert.IConverter;
import com.tibco.as.util.convert.Settings;

public class Destination implements IDestination {

	private ConverterFactory converterFactory = new ConverterFactory();
	private IChannel channel;
	private ExportConfig exportConfig = new ExportConfig();
	private ImportConfig importConfig = new ImportConfig();
	private SpaceDef spaceDef = SpaceDef.create();

	public Destination(IChannel channel) {
		this.channel = channel;
	}

	@Override
	public ExportConfig getExportConfig() {
		return exportConfig;
	}

	@Override
	public ImportConfig getImportConfig() {
		return importConfig;
	}

	public IInputStream getInputStream() {
		return null;
	}

	public IOutputStream getOutputStream() {
		return null;
	}

	public String getName() {
		return getSpaceDef().getName();
	}

	public IChannel getChannel() {
		return channel;
	}

	public IAccessor[] getObjectAccessors(TransferConfig transfer) {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		for (int index = 0; index < getFieldDefs(transfer).size(); index++) {
			accessors.add(new ObjectArrayAccessor(index));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	public IAccessor[] getTupleAccessors(TransferConfig transfer) {
		Collection<IAccessor> accessors = new ArrayList<IAccessor>();
		for (FieldDef fieldDef : getFieldDefs(transfer)) {
			String fieldName = fieldDef.getName();
			FieldType fieldType = fieldDef.getType();
			accessors.add(converterFactory.getAccessor(fieldName, fieldType));
		}
		return accessors.toArray(new IAccessor[accessors.size()]);
	}

	protected IConverter getConverter(Class<?> from, Class<?> to) {
		return converterFactory.getConverter(channel.getSettings(), from, to);
	}

	protected IConverter getConverter(FieldType from, Class<?> to) {
		return converterFactory.getConverter(channel.getSettings(), from, to);
	}

	public Collection<FieldDef> getFieldDefs(TransferConfig transfer) {
		if (transfer.getFieldNames().isEmpty()) {
			return getSpaceDef().getFieldDefs();
		}
		Collection<FieldDef> fieldDefs = new ArrayList<FieldDef>();
		for (String fieldName : transfer.getFieldNames()) {
			FieldDef fieldDef = getFieldDef(fieldName);
			if (fieldDef == null) {
				continue;
			}
			fieldDefs.add(fieldDef);
		}
		return fieldDefs;
	}

	protected FieldDef getFieldDef(String fieldName) {
		return spaceDef.getFieldDef(fieldName);
	}

	public String[] getFieldNames(TransferConfig transferConfig) {
		Collection<String> fieldNames = new ArrayList<String>();
		for (FieldDef fieldDef : getFieldDefs(transferConfig)) {
			fieldNames.add(fieldDef.getName());
		}
		return fieldNames.toArray(new String[fieldNames.size()]);
	}

	public IConverter[] getJavaConverters(TransferConfig transfer) {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (FieldDef fieldDef : getFieldDefs(transfer)) {
			converters.add(getJavaConverter(fieldDef));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	private IConverter getJavaConverter(FieldDef fieldDef) {
		Settings settings = channel.getSettings();
		Class<?> from = getJavaType(fieldDef);
		FieldType to = fieldDef.getType();
		return converterFactory.getConverter(settings, from, to);
	}

	protected Class<?> getJavaType(FieldDef fieldDef) {
		return Object.class;
	}

	public IConverter[] getFieldConverters(TransferConfig transfer) {
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (FieldDef fieldDef : getFieldDefs(transfer)) {
			converters.add(getFieldConverter(fieldDef));
		}
		return converters.toArray(new IConverter[converters.size()]);
	}

	private IConverter getFieldConverter(FieldDef fieldDef) {
		Settings settings = channel.getSettings();
		FieldType from = fieldDef.getType();
		Class<?> to = getJavaType(fieldDef);
		return converterFactory.getConverter(settings, from, to);
	}

	@Override
	public SpaceDef getSpaceDef() {
		return spaceDef;
	}

	@Override
	public void setSpaceDef(SpaceDef spaceDef) {
		this.spaceDef = spaceDef;
	}

}