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
		List<Object[]> list = new Vector<Object[]>();
		ChannelConfig channelConfig = getChannelConfig();
		TestConfig export = new TestConfig();
		export.setDirection(Direction.EXPORT);
		export.setWorkerCount(5);
		export.setBatchSize(7000);
		export.setQueueCapacity(35000);
		export.setQueryLimit(100000L);
		export.setSpace(space.getName());
		export.setOutputStream(new ListOutputStream<Object[]>(list));
		channelConfig.getDestinations().add(export);
		TestChannel channel = new TestChannel(channelConfig);
		channel.open();
		channel.close();
		Assert.assertEquals(space.size(), list.size());
		for (Object[] line : list) {
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
		List<Object[]> list = new Vector<Object[]>();
		ListOutputStream<Object[]> out = new ListOutputStream<Object[]>(list);
		out.setSleep(100);
		ChannelConfig channelConfig = getChannelConfig();
		TestConfig export = new TestConfig();
		export.setDirection(Direction.EXPORT);
		export.setSpace(space.getName());
		export.setTimeScope(TimeScope.ALL);
		export.setTimeout(100L);
		export.setWorkerCount(1);
		export.setBatchSize(1);
		export.setQueueCapacity(1);
		export.setOutputStream(out);
		channelConfig.getDestinations().add(export);
		TestChannel channel = new TestChannel(channelConfig);
		channel.addListener(new ChannelAdapter() {

			@Override
			public void opened(IDestination destination) {
				try {
					destination.stop();
				} catch (Exception e) {
					e.printStackTrace();
				}
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
