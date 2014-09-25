package com.tibco.as.io;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.junit.Assert;
import org.junit.Test;

import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public class TestImporter extends TestBase {

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
		TestConfig config = new TestConfig();
		config.setDirection(Direction.IMPORT);
		config.setSpaceName(spaceName);
		config.setInputStream(in);
		TestChannel channel = new TestChannel(metaspace);
		channel.addConfig(config);
		channel.open();
		channel.close();
		Assert.assertEquals(3, space.size());
		Tuple tuple1 = Tuple.create();
		tuple1.putString("guid", "1");
		Assert.assertEquals(calendar1.getTime(),
				space.get(tuple1).getDateTime("last-payment").getTime()
						.getTime());
	}
}
