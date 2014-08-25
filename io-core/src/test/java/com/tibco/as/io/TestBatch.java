package com.tibco.as.io;

import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.space.ASException;
import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.browser.BrowserDef.TimeScope;
import com.tibco.as.util.Utils;

public class TestBatch extends TestBase {

	private ConverterFactory factory = new ConverterFactory();

	private Space space;

	@Before
	public void setup() throws ASException {
		SpaceDef spaceDef = SpaceDef.create("cust_account");
		Collection<FieldDef> fieldDefs = spaceDef.getFieldDefs();
		fieldDefs.add(FieldDef.create("guid", FieldType.STRING));
		fieldDefs.add(FieldDef.create("last-payment", FieldType.DATETIME));
		fieldDefs.add(FieldDef.create("average-spend", FieldType.DOUBLE));
		spaceDef.setKey("guid");
		Metaspace metaspace = getMetaspace();
		metaspace.defineSpace(spaceDef);
		String spaceName = spaceDef.getName();
		space = metaspace.getSpace(spaceName, DistributionRole.SEEDER);
		for (int index = 0; index < 70000; index++) {
			Tuple tuple = Tuple.create();
			tuple.putString("guid", String.valueOf(index + 1));
			tuple.putDateTime("last-payment",
					DateTime.create(Calendar.getInstance()));
			tuple.putDouble("average-spend", index);
			space.put(tuple);
		}
	}

	@Test
	public void testBatch() throws Exception {
		final List<String[]> list = new Vector<String[]>();
		Exporter<String[]> exporter = new Exporter<String[]>(getMetaspace()) {

			@Override
			protected IOutputStream<String[]> getOutputStream(
					Metaspace metaspace, Transfer transfer, SpaceDef spaceDef) {
				return new ListOutputStream<String[]>(list);
			}

			@SuppressWarnings("rawtypes")
			@Override
			protected IConverter<Tuple, String[]> getConverter(
					Transfer transfer, SpaceDef spaceDef)
					throws UnsupportedConversionException {
				FieldDef[] fieldDefs = Utils.getFieldDefs(spaceDef);
				ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
				IConverter[] converters;
				try {
					converters = factory.getConverters(
							transfer.getAttributes(), fieldDefs, String.class);
				} catch (Exception e) {
					throw new UnsupportedConversionException(Tuple.class,
							String[].class);
				}
				return new TupleToArrayConverter<String>(accessors, converters,
						String.class);
			}

		};
		Export defaultExport = new Export();
		defaultExport.setWorkerCount(5);
		defaultExport.setBatchSize(7000);
		defaultExport.setQueueCapacity(35000);
		defaultExport.setQueryLimit(100000L);
		exporter.setDefaultTransfer(defaultExport);
		exporter.execute();
		Assert.assertEquals(space.size(), list.size());
		for (String[] line : list) {
			Assert.assertNotNull(line);
			Assert.assertEquals(3, line.length);
			Assert.assertNotNull(line[0]);
			Assert.assertNotNull(line[1]);
			Assert.assertNotNull(line[2]);
		}
	}

	@Test
	public void testStopTransfer() throws Exception {
		List<String[]> list = new Vector<String[]>();
		ListOutputStream<String[]> out = new ListOutputStream<String[]>(list);
		out.setSleep(100);
		final Exporter<String[]> exporter = new Exporter<String[]>(
				getMetaspace()) {

			@SuppressWarnings("rawtypes")
			@Override
			protected IConverter<Tuple, String[]> getConverter(
					Transfer transfer, SpaceDef spaceDef)
					throws UnsupportedConversionException {
				FieldDef[] fieldDefs = Utils.getFieldDefs(spaceDef);
				ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
				IConverter[] converters = factory.getConverters(
						transfer.getAttributes(), fieldDefs, String.class);
				return new TupleToArrayConverter<String>(accessors, converters,
						String.class);
			}

		};
		Export export = new Export();
		export.setSpaceName(space.getName());
		export.setTimeScope(TimeScope.ALL);
		export.setTimeout(100L);
		export.setWorkerCount(1);
		export.setBatchSize(1);
		export.setQueueCapacity(1);
		exporter.setOutputStream(out);
		exporter.addTransfer(export);
		exporter.addListener(new IMetaspaceTransferListener() {

			@Override
			public void opening(Collection<ITransfer> transfers) {
			}

			@Override
			public void executing(ITransfer transfer) {
				transfer.addListener(new ITransferListener() {

					@Override
					public void transferred(int count) {
						try {
							exporter.stop();
						} catch (Exception e) {
							Assert.fail("Got exception on stop()");
						}
					}

					@Override
					public void opened() {
					}

					@Override
					public void closed() {
					}
				});
			}

		});
		exporter.execute();
		Assert.assertTrue(list.size() <= 15);
	}

	@After
	public void teardown() throws ASException {
		space.close();
	}
}
