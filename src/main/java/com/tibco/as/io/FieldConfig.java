package com.tibco.as.io;

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

}
