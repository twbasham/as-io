package com.tibco.as.io;

import com.tibco.as.space.FieldDef.FieldType;

public class Field implements Cloneable {

	private String name;

	private FieldType type;

	private boolean encrypted;

	private boolean nullable;

	private boolean key;

	public Field() {
	}

	public Field(String name, FieldType type) {
		this.name = name;
		this.type = type;
	}

	public Field(String name, FieldType type, boolean key) {
		this(name, type);
		this.key = key;
	}

	public Field(String name, FieldType type, boolean key, boolean nullable) {
		this(name, type, key);
		this.nullable = nullable;
	}

	public Field clone() {
		Field field = new Field();
		copyTo(field);
		return field;
	}

	public void copyTo(Field field) {
		field.encrypted = encrypted;
		field.key = key;
		field.name = name;
		field.nullable = nullable;
		field.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public FieldType getType() {
		return type;
	}

	public void setType(FieldType type) {
		this.type = type;
	}

	public boolean isEncrypted() {
		return encrypted;
	}

	public void setEncrypted(boolean encrypted) {
		this.encrypted = encrypted;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public boolean isKey() {
		return key;
	}

	public void setKey(boolean key) {
		this.key = key;
	}

	public void setType(String name) {
		setType(FieldType.valueOf(name));
	}

}
