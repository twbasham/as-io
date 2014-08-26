package com.tibco.as.io;

import java.text.MessageFormat;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.Attributes;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;

public class FieldUtils {

	private static Logger logger = Logger.getLogger(FieldUtils.class.getName());

	public static FieldDef getFieldDef(Field field) {
		if (field.getName() == null) {
			return null;
		}
		FieldType type = field.getType();
		if (type == null) {
			type = FieldType.STRING;
		}
		FieldDef fieldDef = FieldDef.create(field.getName(), type);
		fieldDef.setNullable(field.isNullable());
		fieldDef.setEncrypted(field.isEncrypted());
		return fieldDef;

	}

	public static FieldDef[] getFieldDefs(Field[] fields) {
		FieldDef[] fieldDefs = new FieldDef[fields.length];
		for (int index = 0; index < fields.length; index++) {
			fieldDefs[index] = getFieldDef(fields[index]);
		}
		return fieldDefs;
	}

	public static FieldDef[] getFieldDefs(SpaceDef spaceDef, Field[] fields) {
		Field[] array = getFields(fields, spaceDef);
		FieldDef[] fieldDefs = new FieldDef[array.length];
		for (int index = 0; index < array.length; index++) {
			fieldDefs[index] = getFieldDef(array[index]);
		}
		return fieldDefs;
	}

	public static Field[] getFields(Field[] fields, SpaceDef spaceDef) {
		Field[] result = fields;
		if (fields == null || fields.length == 0) {
			FieldDef[] fieldDefs = getFieldDefs(spaceDef);
			result = new Field[fieldDefs.length];
			for (int index = 0; index < fieldDefs.length; index++) {
				result[index] = new Field();
				result[index].setName(fieldDefs[index].getName());
			}
		}
		for (Field field : result) {
			if (field.getName() != null) {
				getField(spaceDef, field.getName()).copyTo(field);
			}
		}
		return result;
	}

	private static FieldDef[] getFieldDefs(SpaceDef spaceDef) {
		return spaceDef.getFieldDefs().toArray(
				new FieldDef[spaceDef.getFieldDefs().size()]);
	}

	public static Field getField(SpaceDef spaceDef, String fieldName) {
		Field field = new Field();
		FieldDef fieldDef = spaceDef.getFieldDef(fieldName);
		field.setName(fieldDef.getName());
		field.setEncrypted(fieldDef.isEncrypted());
		field.setKey(spaceDef.getKeyDef().getFieldNames()
				.contains(fieldDef.getName()));
		field.setNullable(fieldDef.isNullable());
		field.setType(fieldDef.getType());
		return field;
	}

	public static SpaceDef createSpaceDef(String spaceName, Field[] fields) {
		SpaceDef spaceDef = SpaceDef.create(spaceName);
		for (FieldDef fieldDef : getFieldDefs(fields)) {
			if (fieldDef != null) {
				spaceDef.getFieldDefs().add(fieldDef);
			}
		}
		for (Field field : fields) {
			if (field.isKey()) {
				spaceDef.getKeyDef().getFieldNames().add(field.getName());
			}
		}
		return spaceDef;
	}

	public static Field[] getFields(String[] fieldDefs) {
		FieldFormat format = new FieldFormat();
		Field[] fields = new Field[fieldDefs.length];
		for (int index = 0; index < fieldDefs.length; index++) {
			Field field = new Field();
			String fieldDef = fieldDefs[index];
			if (fieldDef != null && !fieldDef.isEmpty()) {
				try {
					field = format.parseObject(fieldDef);
				} catch (ParseException e) {
					logger.log(Level.SEVERE, MessageFormat.format(
							"Could not parse field def {0}", fieldDef), e);
				}
			}
			fields[index] = field;
		}
		return fields;
	}

	@SuppressWarnings("rawtypes")
	public static <T> TupleToArrayConverter<T> getTupleToArrayConverter(
			ConverterFactory converterFactory, SpaceDef spaceDef,
			Field[] fields, Class<T> componentType, Attributes attributes,
			Class[] types) throws UnsupportedConversionException {
		ITupleAccessor[] accessors = AccessorFactory.create(getFieldDefs(
				spaceDef, fields));
		IConverter[] converters = converterFactory.getConverters(attributes,
				getFieldDefs(spaceDef, fields), types);
		return new TupleToArrayConverter<T>(accessors, converters,
				componentType);
	}

	@SuppressWarnings("rawtypes")
	public static <T> ArrayToTupleConverter<T> getArrayToTupleConverter(
			ConverterFactory converterFactory, SpaceDef spaceDef,
			Field[] fields, Class<T> componentType, Attributes attributes)
			throws UnsupportedConversionException {
		FieldDef[] fieldDefs = getFieldDefs(spaceDef, fields);
		ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
		IConverter[] converters = converterFactory.getConverters(attributes,
				componentType, fieldDefs);
		return new ArrayToTupleConverter<T>(accessors, converters);
	}

	public static String[] format(Field[] fields) {
		FieldFormat format = new FieldFormat();
		String[] fieldDefs = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			fieldDefs[index] = format.format(fields[index]);
		}
		return fieldDefs;
	}
}
