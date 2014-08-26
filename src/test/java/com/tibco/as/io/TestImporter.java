package com.tibco.as.io;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.util.Utils;

public class TestImporter extends TestBase {

	private ConverterFactory factory = new ConverterFactory();

	@Test
	public void testListImporter() throws Exception {
		String spaceName = "cust_account";
		SpaceDef spaceDef = SpaceDef.create(spaceName);
		List<FieldDef> fieldDefs = Arrays.asList(
				FieldDef.create("guid", FieldType.STRING),
				FieldDef.create("last-payment", FieldType.DATETIME),
				FieldDef.create("average-spend", FieldType.DOUBLE));
		spaceDef.getFieldDefs().addAll(fieldDefs);
		spaceDef.setKey("guid");
		Calendar calendar1 = Calendar.getInstance();
		calendar1.clear();
		calendar1.set(2013, 10, 1);
		Calendar calendar2 = Calendar.getInstance();
		calendar2.clear();
		calendar2.set(2013, 10, 2);
		Calendar calendar3 = Calendar.getInstance();
		calendar3.clear();
		calendar3.set(2013, 10, 3);
		List<String[]> list = Arrays.asList(new String[] { "1",
				DatatypeConverter.printDateTime(calendar1), "1.11" },
				new String[] { "2", DatatypeConverter.printDateTime(calendar2),
						"2.22" },
				new String[] { "3", DatatypeConverter.printDateTime(calendar3),
						"3.33" });
		Metaspace metaspace = getMetaspace();
		metaspace.defineSpace(spaceDef);
		Space space = metaspace.getSpace(spaceDef.getName(),
				DistributionRole.SEEDER);
		ListInputStream<String[]> in = new ListInputStream<String[]>(list);
		in.setSleep(103);
		Importer<String[]> importer = new Importer<String[]>(metaspace) {

			@Override
			protected String getInputSpaceName(Import config) {
				return null;
			}

			@Override
			protected IInputStream<String[]> getInputStream(
					Metaspace metaspace, Transfer transfer, SpaceDef spaceDef) {
				return null;
			}

			@SuppressWarnings("rawtypes")
			@Override
			protected IConverter<String[], Tuple> getConverter(
					Transfer transfer, SpaceDef spaceDef)
					throws UnsupportedConversionException {
				FieldDef[] fieldDefs = Utils.getFieldDefs(spaceDef);
				ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
				IConverter[] converters;
				try {
					converters = factory.getConverters(
							transfer.getAttributes(), String.class, fieldDefs);
				} catch (Exception e) {
					throw new UnsupportedConversionException(String[].class,
							Tuple.class);
				}
				return new ArrayToTupleConverter<String>(accessors, converters);
			}

			@Override
			protected SpaceDef createSpaceDef(String spaceName, Import config) {
				return SpaceDef.create(spaceName);
			}
		};
		Import config = new Import();
		config.setSpaceName(spaceName);
		importer.setInputStream(in);
		importer.addTransfer(config);
		importer.execute();
		Assert.assertEquals(3, space.size());
		Tuple tuple1 = Tuple.create();
		tuple1.putString("guid", "1");
		Assert.assertEquals(calendar1.getTime(),
				space.get(tuple1).getDateTime("last-payment").getTime()
						.getTime());
	}
}
