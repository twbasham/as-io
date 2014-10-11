package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.Attributes;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;

public abstract class AbstractDestination implements IDestination {

	private ConverterFactory converterFactory = new ConverterFactory();
	private AbstractChannel channel;
	private DestinationConfig config;
	private IInputStream in;
	private ExecutorService service;
	private boolean closed;
	private Collection<Worker> workers = new ArrayList<Worker>();

	protected AbstractDestination(AbstractChannel channel,
			DestinationConfig config) {
		this.channel = channel;
		this.config = config;
	}

	protected DestinationConfig getConfig() {
		return config;
	}

	protected IChannel getChannel() {
		return channel;
	}

	@Override
	public void open(Metaspace metaspace) throws Exception {
		in = getInputStream(metaspace);
		in.open();
		int workerCount = config.getWorkerCount();
		service = Executors.newFixedThreadPool(workerCount);
		for (int index = 0; index < workerCount; index++) {
			IOutputStream out = getOutputStream(metaspace);
			out.open();
			IConverter converter = getConverter();
			Worker worker = new Worker(in, converter, out);
			workers.add(worker);
			service.execute(worker);
		}
		closed = false;
	}

	private IInputStream getInputStream(Metaspace metaspace) throws Exception {
		if (isImport()) {
			return getInputStream();
		}
		return new SpaceInputStream(metaspace, config);
	}

	private IOutputStream getOutputStream(Metaspace metaspace) throws Exception {
		if (isImport()) {
			int batchSize = config.getSpaceBatchSize();
			if (batchSize > 1) {
				return new BatchSpaceOutputStream(metaspace, config, batchSize);
			}
			return new SpaceOutputStream(metaspace, config);
		}
		return getOutputStream();
	}

	private IConverter getConverter() throws UnsupportedConversionException {
		Collection<ITupleAccessor> al = new ArrayList<ITupleAccessor>();
		Collection<IConverter> cl = new ArrayList<IConverter>();
		for (FieldConfig field : config.getFields()) {
			String fieldName = field.getFieldName();
			FieldType fieldType = field.getFieldType();
			al.add(AccessorFactory.create(fieldName, fieldType));
			cl.add(getConverter(fieldName, fieldType, field.getJavaType()));
		}
		ITupleAccessor[] accessors = al.toArray(new ITupleAccessor[al.size()]);
		IConverter[] converters = cl.toArray(new IConverter[cl.size()]);
		if (isImport()) {
			return new ArrayToTupleConverter(accessors, converters);
		}
		return new TupleToArrayConverter(accessors, converters);
	}

	private IConverter getConverter(String fieldName, FieldType fieldType,
			Class<?> type) throws UnsupportedConversionException {
		Attributes attributes = config.getAttributes().getAttributes(fieldName);
		Class<?> from = isImport() ? type : ConverterFactory.getType(fieldType);
		Class<?> to = isImport() ? ConverterFactory.getType(fieldType) : type;
		return converterFactory.getConverter(attributes, from, to);
	}

	private boolean isImport() {
		return config.getDirection() == Direction.IMPORT;
	}

	protected abstract IInputStream getInputStream() throws Exception;

	protected abstract IOutputStream getOutputStream() throws Exception;

	@Override
	public void close() throws Exception {
		service.shutdown();
		try {
			while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
				// do nothing
			}
		} catch (InterruptedException e) {
			throw new Exception("Could not finish transfers", e);
		}
		in.close();
		closed = true;
		if (config.getDistributionRole() == DistributionRole.SEEDER) {
			return;
		}
		workers.clear();
	}

	@Override
	public void stop() throws Exception {
		in.close();
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public long size() {
		if (in == null) {
			return IInputStream.UNKNOWN_SIZE;
		}
		return in.size();
	}

	@Override
	public long getPosition() {
		if (in == null) {
			return 0;
		}
		return in.getPosition();
	}

	@Override
	public String getName() {
		return config.getSpace();
	}

}