package com.tibco.as.io;

import java.text.FieldPosition;
import java.text.Format;
import java.text.ParseException;
import java.text.ParsePosition;

public class FieldFormat extends Format {

	private static final long serialVersionUID = -2534441437805237858L;

	private static final char BRACKET_OPEN = '[';
	private static final char BRACKET_CLOSE = ']';
	private static final char SEPARATOR = ' ';
	private static final String NULLABLE = "nullable";
	private static final String ENCRYPTED = "encrypted";
	private static final String KEY = "key";

	public String format(com.tibco.as.io.Field field) {
		return super.format(field);
	}

	@Override
	public com.tibco.as.io.Field parseObject(String source)
			throws ParseException {
		return (com.tibco.as.io.Field) super.parseObject(source);
	}

	@Override
	public StringBuffer format(Object obj, StringBuffer buffer,
			FieldPosition pos) {
		com.tibco.as.io.Field field = (com.tibco.as.io.Field) obj;
		if (field.getName() != null) {
			buffer.append(field.getName());
		}
		if (field.getType() != null) {
			buffer.append(BRACKET_OPEN);
			buffer.append(field.getType().name());
			if (field.isNullable()) {
				buffer.append(SEPARATOR);
				buffer.append(NULLABLE);
			}
			if (field.isEncrypted()) {
				buffer.append(SEPARATOR);
				buffer.append(ENCRYPTED);
			}
			if (field.isKey()) {
				buffer.append(SEPARATOR);
				buffer.append(KEY);
			}
			buffer.append(BRACKET_CLOSE);
		}
		return buffer;
	}

	@Override
	public com.tibco.as.io.Field parseObject(String source, ParsePosition pos) {
		if (source == null) {
			return null;
		}
		com.tibco.as.io.Field field = new com.tibco.as.io.Field();
		int openPos = source.indexOf(BRACKET_OPEN);
		if (openPos > 0) {
			field.setName(source.substring(0, openPos).trim());
			if (openPos + 1 < source.length()) {
				int closePos = source.indexOf(BRACKET_CLOSE);
				if (closePos > 0) {
					String attributes = source.substring(openPos + 1, closePos)
							.trim();
					int separatorPos = attributes.indexOf(SEPARATOR);
					if (separatorPos > 0) {
						field.setType(attributes.substring(0, separatorPos)
								.trim());
						if (separatorPos + 1 < attributes.length()) {
							attributes = attributes.substring(separatorPos);
							field.setNullable(attributes.contains(NULLABLE));
							field.setEncrypted(attributes.contains(ENCRYPTED));
							field.setKey(attributes.contains(KEY));
						}
					} else {
						field.setType(attributes);
					}
				}
			}
		} else {
			String name = source.trim();
			if (!name.isEmpty()) {
				field.setName(name);
			}
		}
		pos.setIndex(source.length());
		return field;
	}
}
