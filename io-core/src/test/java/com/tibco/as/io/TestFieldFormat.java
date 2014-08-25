package com.tibco.as.io;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.junit.Test;

import com.tibco.as.io.Field;
import com.tibco.as.io.FieldFormat;
import com.tibco.as.space.FieldDef.FieldType;

public class TestFieldFormat {

	@Test
	public void testFormatObject() {
		FieldFormat format = new FieldFormat();
		Field field = new Field();
		field.setName("field-1");
		assertEquals("field-1", format.format(field));
		field.setType(FieldType.DATETIME);
		assertEquals("field-1[DATETIME]", format.format(field));
		field.setEncrypted(false);
		assertEquals("field-1[DATETIME]", format.format(field));
		field.setEncrypted(true);
		assertEquals("field-1[DATETIME encrypted]", format.format(field));
		field.setKey(true);
		assertEquals("field-1[DATETIME encrypted key]", format.format(field));
		field.setNullable(true);
		assertEquals("field-1[DATETIME nullable encrypted key]",
				format.format(field));
	}

	@Test
	public void testParseObjectString() throws ParseException {
		FieldFormat format = new FieldFormat();
		Field field = format.parseObject("field-1");
		assertEquals("field-1", field.getName());
		assertEquals(null, field.getType());
		assertEquals(false, field.isKey());
		assertEquals(false, field.isEncrypted());
		assertEquals(false, field.isNullable());
		field = format.parseObject("field-1[DATETIME]");
		assertEquals("field-1", field.getName());
		assertEquals(FieldType.DATETIME, field.getType());
		assertEquals(false, field.isKey());
		assertEquals(false, field.isEncrypted());
		assertEquals(false, field.isNullable());
		field = format.parseObject("field-1[DATETIME key]");
		assertEquals("field-1", field.getName());
		assertEquals(FieldType.DATETIME, field.getType());
		assertEquals(true, field.isKey());
		assertEquals(false, field.isEncrypted());
		assertEquals(false, field.isNullable());
		field = format.parseObject("field-1[DATETIME nullable encrypted key]");
		assertEquals("field-1", field.getName());
		assertEquals(FieldType.DATETIME, field.getType());
		assertEquals(true, field.isKey());
		assertEquals(true, field.isEncrypted());
		assertEquals(true, field.isNullable());
	}

}
