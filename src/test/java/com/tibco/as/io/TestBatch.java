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
import com.tibco.as.util.BrowserConfig;

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
		TestChannel channel = getChannel();
		TestDestination destination = channel.newDestination();
		channel.getDestinations().add(destination);
		destination.setSpaceName(space.getName());
		destination.getExportConfig().setWorkerCount(5);
		destination.getExportConfig().getBrowserConfig().setQueryLimit(100000L);
		ChannelExport export = channel.getExport();
		export.prepare();
		export.execute();
		List<Object> list = destination.getList();
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
		TestDestination destination = (TestDestination) channel
				.newDestination();
		channel.getDestinations().add(destination);
		destination.setSleep(100L);
		destination.setSpaceName(space.getName());
		ExportConfig exportConfig = destination.getExportConfig();
		BrowserConfig browserConfig = exportConfig.getBrowserConfig();
		browserConfig.setTimeScope(TimeScope.ALL);
		browserConfig.setTimeout(100L);
		exportConfig.setWorkerCount(1);
		ChannelExport transfer = channel.getExport();
		transfer.addListener(new IChannelTransferListener() {

			@Override
			public void executing(final IDestinationTransfer transfer) {
				((TestDestination) transfer.getDestination())
						.addListener(new IOutputStreamListener() {

							@Override
							public void wrote(Object object) {
								try {
									System.out.println("Stopping transfer");
									transfer.stop();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
			}
		});
		transfer.prepare();
		transfer.execute();
		Assert.assertTrue(destination.getList().size() <= 15);
	}

	@After
	public void teardown() throws ASException {
		space.close();
	}
}
