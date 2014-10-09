package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;

import javax.xml.bind.DatatypeConverter;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.space.DateTime;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class TestExport extends TestBase {

	@Test
	public void testExporter() throws Exception {
		String spaceName = "cust_account";
		SpaceDef spaceDef = SpaceDef.create(spaceName, 0, Arrays.asList(
				FieldDef.create("guid", FieldType.STRING),
				FieldDef.create("last-payment", FieldType.DATETIME),
				FieldDef.create("average-spend", FieldType.DOUBLE)));
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
		List<Object[]> outList = new Vector<Object[]>();
		ListOutputStream<Object[]> out = new ListOutputStream<Object[]>(outList);
		out.setSleep(140);
		ChannelConfig channelConfig = getChannelConfig();
		TestConfig export = new TestConfig();
		export.setDirection(Direction.EXPORT);
		export.setSpace(spaceName);
		export.setOutputStream(out);
		channelConfig.getDestinations().add(export);
		TestChannel channel = new TestChannel(channelConfig);
		channel.open();
		channel.close();
		Assert.assertEquals(3, outList.size());
		for (Object[] line : outList) {
			Assert.assertEquals(3, line.length);
			Calendar calendar = Calendar.getInstance();
			Object guid = line[0];
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
					.parseDateTime((String) line[1]).getTimeInMillis());
		}
	}
}
