package com.tibco.as.io;

import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;

public class FieldConfig implements Cloneable {

	private String fieldName;
	private Boolean fieldNullable;
	private Boolean fieldEncrypted;
	private FieldType fieldType;

	@Override
	public FieldConfig clone() {
		FieldConfig field = new FieldConfig();
		copyTo(field);
		return field;
	}

	public void copyTo(FieldConfig target) {
		target.fieldEncrypted = fieldEncrypted;
		target.fieldName = fieldName;
		target.fieldNullable = fieldNullable;
		target.fieldType = fieldType;
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

	public FieldType getFieldType() {
		return fieldType;
	}

	public void setFieldType(FieldType fieldType) {
		this.fieldType = fieldType;
	}

	public Class<?> getJavaType() {
		return ConverterFactory.getType(getFieldType());
	}

	public FieldDef getFieldDef() {
		FieldDef fieldDef = FieldDef.create(getFieldName(), getFieldType());
		Boolean encrypted = getFieldEncrypted();
		if (encrypted != null) {
			fieldDef.setEncrypted(encrypted);
		}
		Boolean nullable = getFieldNullable();
		if (nullable != null) {
			fieldDef.setNullable(nullable);
		}
		return fieldDef;
	}

	public void setFieldDef(FieldDef fieldDef) {
		setFieldName(fieldDef.getName());
		setFieldType(fieldDef.getType());
		setFieldNullable(fieldDef.isNullable());
		setFieldEncrypted(fieldDef.isEncrypted());
	}

}
