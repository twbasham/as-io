package com.tibco.as.io;

import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.util.Field;
import com.tibco.as.util.FieldFormat;

public class FieldUtils {

	private static Logger logger = LogFactory.getLog(FieldUtils.class);

	public static FieldDef getFieldDef(Field field) {
		if (field == null) {
			return null;
		}
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

	public static FieldDef[] getFieldDefs(SpaceDef spaceDef, String... fields) {
		if (fields.length == 0) {
			return getFieldDefs(spaceDef);
		}
		FieldDef[] fieldDefs = new FieldDef[fields.length];
		for (int index = 0; index < fields.length; index++) {
			fieldDefs[index] = spaceDef.getFieldDef(fields[index]);
		}
		return fieldDefs;
	}

	public static Field[] getFields(Field[] fields, SpaceDef spaceDef) {
		Collection<Field> result = new ArrayList<Field>();
		if (fields == null || fields.length == 0) {
			for (FieldDef fieldDef : getFieldDefs(spaceDef)) {
				result.add(new Field(fieldDef.getName()));
			}
		} else {
			for (Field field : fields) {
				result.add(field);
			}
		}
		for (Field field : result) {
			if (field == null) {
				continue;
			}
			if (field.getName() == null) {
				continue;
			}
			getField(spaceDef, field.getName()).copyTo(field);
		}
		return result.toArray(new Field[result.size()]);
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

	public static SpaceDef populateSpaceDef(SpaceDef spaceDef, Field[] fields) {
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

	public static String[] format(Field[] fields) {
		FieldFormat format = new FieldFormat();
		String[] fieldDefs = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			fieldDefs[index] = format.format(fields[index]);
		}
		return fieldDefs;
	}
}
