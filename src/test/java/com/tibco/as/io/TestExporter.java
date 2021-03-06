package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;

import javax.xml.bind.DatatypeConverter;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.util.Field;

public class TestExporter extends TestBase {

	private ConverterFactory converterFactory = new ConverterFactory();

	@Test
	public void testExporter() throws Exception {
		String spaceName = "cust_account";
		FieldDef unused = FieldDef.create("unused", FieldType.STRING);
		unused.setNullable(true);
		SpaceDef spaceDef = SpaceDef.create(spaceName, 0, Arrays.asList(
				FieldDef.create("guid", FieldType.STRING),
				FieldDef.create("last-payment", FieldType.DATETIME),
				FieldDef.create("average-spend", FieldType.DOUBLE), unused));
		spaceDef.setKey("guid");
		SpaceDef spaceDef2 = SpaceDef.create("space2", 0, Arrays.asList(
				FieldDef.create("guid", FieldType.STRING),
				FieldDef.create("last-payment", FieldType.DATETIME)));
		spaceDef2.setKey("guid");
		Calendar calendar1 = Calendar.getInstance();
		calendar1.clear();
		calendar1.set(2013, 10, 1);
		Calendar calendar2 = Calendar.getInstance();
		calendar2.clear();
		calendar2.set(2013, 10, 2);
		Calendar calendar3 = Calendar.getInstance();
		calendar3.clear();
		calendar3.set(2013, 10, 3);
		List<Tuple> list = new ArrayList<Tuple>();
		Tuple tuple1 = Tuple.create();
		tuple1.putString("guid", "1");
		tuple1.putDateTime("last-payment", DateTime.create(calendar1));
		tuple1.putDouble("average-spend", 1.11);
		Tuple tuple2 = Tuple.create();
		tuple2.putString("guid", "2");
		tuple2.putDateTime("last-payment", DateTime.create(calendar2));
		tuple2.putDouble("average-spend", 2.22);
		Tuple tuple3 = Tuple.create();
		tuple3.putString("guid", "3");
		tuple3.putDateTime("last-payment", DateTime.create(calendar3));
		tuple3.putDouble("average-spend", 3.33);
		list.add(tuple1);
		list.add(tuple2);
		list.add(tuple3);
		Metaspace metaspace = getMetaspace();
		metaspace.defineSpace(spaceDef);
		metaspace.defineSpace(spaceDef2);
		Space space = metaspace.getSpace(spaceDef.getName(),
				DistributionRole.SEEDER);
		space.putAll(list);
		Space space2 = metaspace.getSpace(spaceDef2.getName(),
				DistributionRole.SEEDER);
		space2.putAll(list);
		List<String[]> outList = new Vector<String[]>();
		final ListOutputStream<String[]> out = new ListOutputStream<String[]>(
				outList);
		out.setSleep(140);
		final DestinationConfig export = new DestinationConfig();
		export.setDirection(Direction.EXPORT);
		export.setSpaceName(spaceName);
		TestChannel channel = new TestChannel(getMetaspace());
		AbstractDestination<String[]> exporter = new AbstractDestination<String[]>(
				channel, export) {

			@Override
			protected IConverter<Tuple, String[]> getExportConverter(
					DestinationConfig config, SpaceDef spaceDef)
					throws UnsupportedConversionException {
				Field[] fields = new Field[3];
				fields[0] = new Field("guid");
				fields[1] = new Field("last-payment");
				fields[2] = new Field("average-spend");
				FieldDef[] fieldDefs = FieldUtils
						.getFieldDefs(spaceDef, fields);
				ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
				@SuppressWarnings("rawtypes")
				IConverter[] converters;
				try {
					converters = converterFactory.getConverters(
							export.getAttributes(), fieldDefs, String.class);
				} catch (Exception e) {
					throw new UnsupportedConversionException(Tuple.class,
							String[].class);
				}
				return new TupleToArrayConverter<String>(accessors, converters,
						String.class);
			}

			@Override
			protected IConverter<String[], Tuple> getImportConverter(
					DestinationConfig config, SpaceDef spaceDef)
					throws UnsupportedConversionException {
				return null;
			}

			@Override
			protected IInputStream<String[]> getInputStream(
					DestinationConfig config) throws Exception {
				return null;
			}

			@Override
			protected IOutputStream<String[]> getOutputStream(
					DestinationConfig config, SpaceDef spaceDef)
					throws Exception {
				return out;
			}

			@Override
			protected void populateSpaceDef(SpaceDef spaceDef,
					DestinationConfig config) throws Exception {

			}

		};
		exporter.open();
		exporter.close();
		Assert.assertEquals(3, outList.size());
		for (String[] line : outList) {
			Assert.assertEquals(3, line.length);
			Calendar calendar = Calendar.getInstance();
			String guid = line[0];
			if (guid.equals("1")) {
				calendar = calendar1;
			}
			if (guid.equals("2")) {
				calendar = calendar2;
			}
			if (guid.equals("3")) {
				calendar = calendar3;
			}
			Assert.assertEquals(calendar.getTimeInMillis(), DatatypeConverter
					.parseDateTime(line[1]).getTimeInMillis());
		}
	}
}
