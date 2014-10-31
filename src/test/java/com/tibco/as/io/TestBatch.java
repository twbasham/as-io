package com.tibco.as.io;

import java.util.Calendar;
import java.util.Collection;
import java.util.List;

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
		Channel channel = getChannel();
		Metaspace metaspace = channel.getMetaspace();
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
		Channel channel = getChannel();
		TestDestination destination = (TestDestination) channel
				.addDestination();
		destination.setSpace(space.getName());
		destination.setExportWorkerCount(5);
		destination.setQueryLimit(100000L);
		ChannelExport export = channel.getExport();
		export.execute();
		List<Object> list = destination.getOutputStream().getList();
		Assert.assertEquals(space.size(), list.size());
		for (Object element : list) {
			Object[] line = (Object[]) element;
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
		Channel channel = getChannel();
		final TestDestination destination = (TestDestination) channel
				.addDestination();
		destination.setSpace(space.getName());
		destination.setTimeScope(TimeScope.ALL);
		destination.setTimeout(100L);
		destination.setExportWorkerCount(1);
		final ListOutputStream out = destination.getOutputStream();
		out.setSleep(100);
		ChannelExport transfer = channel.getExport();
		transfer.addListener(new IMetaspaceTransferListener() {

			@Override
			public void executing(final AbstractDestinationTransfer transfer) {
				out.addListener(new IOutputStreamListener() {

					@Override
					public void wrote(Object object) {
						try {
							transfer.getInputStream().close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		});
		transfer.execute();
		Assert.assertTrue(out.getList().size() <= 15);
	}

	@After
	public void teardown() throws ASException {
		space.close();
	}
}
