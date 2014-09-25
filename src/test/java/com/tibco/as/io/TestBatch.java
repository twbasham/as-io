package com.tibco.as.io;

import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.tibco.as.log.LogFactory;
import com.tibco.as.log.LogLevel;
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

public class TestBatch extends TestBase {

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
		List<String[]> list = new Vector<String[]>();
		TestConfig export = new TestConfig();
		export.setDirection(Direction.EXPORT);
		export.setWorkerCount(5);
		export.setBatchSize(7000);
		export.setQueueCapacity(35000);
		export.setQueryLimit(100000L);
		export.setSpaceName(space.getName());
		export.setOutputStream(new ListOutputStream<String[]>(list));
		TestChannel channel = new TestChannel(getMetaspace());
		channel.addConfig(export);
		channel.open();
		channel.close();
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
		LogFactory.getRootLogger(LogLevel.VERBOSE);
		List<String[]> list = new Vector<String[]>();
		ListOutputStream<String[]> out = new ListOutputStream<String[]>(list);
		out.setSleep(100);
		TestConfig export = new TestConfig();
		export.setDirection(Direction.EXPORT);
		export.setSpaceName(space.getName());
		export.setTimeScope(TimeScope.ALL);
		export.setTimeout(100L);
		export.setWorkerCount(1);
		export.setBatchSize(1);
		export.setQueueCapacity(1);
		export.setOutputStream(out);
		TestChannel channel = new TestChannel(getMetaspace());
		channel.addListener(new IChannelListener() {

			@Override
			public void opened(IDestination destination) {
				try {
					destination.stop();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			@Override
			public void closed(IDestination destination) {
			}
		});
		channel.open();
		Assert.assertTrue(list.size() <= 15);
	}

	@After
	public void teardown() throws ASException {
		space.close();
	}
}
