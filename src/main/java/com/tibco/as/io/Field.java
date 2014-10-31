package com.tibco.as.io;

import java.util.Calendar;
import java.util.Date;

import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef.FieldType;

public class Field {

	private String fieldName;
	private Boolean fieldNullable;
	private Boolean fieldEncrypted;
	private FieldType fieldType;
	private Class<?> javaType;

	public Class<?> getJavaType() {
		return javaType;
	}

	public void setJavaType(Class<?> javaType) {
		this.javaType = javaType;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	public void setFieldType(FieldType fieldType) {
		this.fieldType = fieldType;
	}

	public Boolean getFieldNullable() {
		return fieldNullable;
	}

	public void setFieldNullable(Boolean nullable) {
		this.fieldNullable = nullable;
	}

	public Boolean getFieldEncrypted() {
		return fieldEncrypted;
	}

	public void setFieldEncrypted(Boolean encrypted) {
		this.fieldEncrypted = encrypted;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	public Field clone() {
		Field field = new Field();
		copyTo(field);
		return field;
	}

	public void copyTo(Field target) {
		target.fieldEncrypted = fieldEncrypted;
		target.fieldName = fieldName;
		target.fieldNullable = fieldNullable;
		target.fieldType = fieldType;
		target.javaType = javaType;
	}

	public FieldType getJavaFieldType() {
		Class<?> javaType = getJavaType();
		if (byte[].class.isAssignableFrom(javaType)) {
			return FieldType.BLOB;
		}
		if (Boolean.class.isAssignableFrom(javaType)) {
			return FieldType.BOOLEAN;
		}
		if (Character.class.isAssignableFrom(javaType)) {
			return FieldType.CHAR;
		}
		if (DateTime.class.isAssignableFrom(javaType)) {
			return FieldType.DATETIME;
		}
		if (Calendar.class.isAssignableFrom(javaType)) {
			return FieldType.DATETIME;
		}
		if (Date.class.isAssignableFrom(javaType)) {
			return FieldType.DATETIME;
		}
		if (Double.class.isAssignableFrom(javaType)) {
			return FieldType.DOUBLE;
		}
		if (Float.class.isAssignableFrom(javaType)) {
			return FieldType.FLOAT;
		}
		if (Integer.class.isAssignableFrom(javaType)) {
			return FieldType.INTEGER;
		}
		if (Long.class.isAssignableFrom(javaType)) {
			return FieldType.LONG;
		}
		if (Short.class.isAssignableFrom(javaType)) {
			return FieldType.SHORT;
		}
		return FieldType.STRING;
	}

}
