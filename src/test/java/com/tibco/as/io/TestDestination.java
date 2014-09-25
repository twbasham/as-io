package com.tibco.as.io;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;
import com.tibco.as.util.Utils;

public class TestDestination extends AbstractDestination<String[]> {

	private ConverterFactory converterFactory = new ConverterFactory();

	public TestDestination(TestChannel channel, TestConfig config) {
		super(channel, config);
	}

	@Override
	protected IInputStream<String[]> getInputStream(DestinationConfig config)
			throws Exception {
		return ((TestConfig) config).getInputStream();
	}

	@Override
	protected IOutputStream<String[]> getOutputStream(DestinationConfig config,
			SpaceDef spaceDef) throws Exception {
		return ((TestConfig) config).getOutputStream();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected IConverter<Tuple, String[]> getExportConverter(
			DestinationConfig config, SpaceDef spaceDef)
			throws UnsupportedConversionException {
		FieldDef[] fieldDefs = Utils.getFieldDefs(spaceDef);
		ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
		IConverter[] converters = converterFactory.getConverters(
				config.getAttributes(), fieldDefs, String.class);
		return new TupleToArrayConverter<String>(accessors, converters,
				String.class);
	}

	@Override
	protected IConverter<String[], Tuple> getImportConverter(
			DestinationConfig config, SpaceDef spaceDef)
			throws UnsupportedConversionException {
		FieldDef[] fieldDefs = Utils.getFieldDefs(spaceDef);
		ITupleAccessor[] accessors = AccessorFactory.create(fieldDefs);
		@SuppressWarnings("rawtypes")
		IConverter[] converters;
		try {
			converters = converterFactory.getConverters(config.getAttributes(),
					String.class, fieldDefs);
		} catch (Exception e) {
			throw new UnsupportedConversionException(String[].class,
					Tuple.class);
		}
		return new ArrayToTupleConverter<String>(accessors, converters);
	}

	@Override
	protected void populateSpaceDef(SpaceDef spaceDef, DestinationConfig config)
			throws Exception {
	}

}
