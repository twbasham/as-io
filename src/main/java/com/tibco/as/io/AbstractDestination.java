package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.io.operation.GetOperation;
import com.tibco.as.io.operation.IOperation;
import com.tibco.as.io.operation.LoadOperation;
import com.tibco.as.io.operation.NoOperation;
import com.tibco.as.io.operation.PartialOperation;
import com.tibco.as.io.operation.PutOperation;
import com.tibco.as.io.operation.TakeOperation;
import com.tibco.as.space.ASException;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.GetOptions;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.PutOptions;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.TakeOptions;
import com.tibco.as.space.Tuple;

public abstract class AbstractDestination implements IDestination {

	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final int DEFAULT_BATCH_SIZE_CONTINUOUS = 1;
	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;
	private static final int DEFAULT_WORKER_COUNT = 1;

	private ConverterFactory converterFactory = new ConverterFactory();
	private AbstractChannel channel;
	private DestinationConfig config;
	private Collection<ITransfer> transfers = new ArrayList<ITransfer>();

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
		transfers.add(getTransfer(metaspace));
		for (ITransfer transfer : transfers) {
			channel.opening(transfer);
			transfer.open();
			channel.opened(transfer);
		}
	}

	private ITransfer getTransfer(Metaspace metaspace) throws Exception {
		if (config.getDirection() == Direction.IMPORT) {
			return getImport(metaspace);
		}
		return getExport(metaspace);
	}

	protected ITransfer getExport(SpaceInputStream in, SpaceDef spaceDef)
			throws Exception {
		int batchSize = getExportBatchSize();
		List<Runnable> workers = new ArrayList<Runnable>();
		for (int index = 0; index < getWorkerCount(); index++) {
			IOutputStream<Object[]> out = getOutputStream();
			IConverter<Tuple, Object[]> converter = getExportConverter(spaceDef);
			workers.add(createWorker(in, converter, out, batchSize));
		}
		String name = getExportName();
		return new Transfer(name, in, workers);
	}

	private <U, V> Worker<U, V> createWorker(IInputStream<U> in,
			IConverter<U, V> converter, IOutputStream<V> out, int batchSize)
			throws Exception {
		if (batchSize > 1) {
			return new BatchWorker<U, V>(in, converter, out, batchSize);
		}
		return new Worker<U, V>(in, converter, out);
	}

	protected String getExportName() {
		return config.getSpace();
	}

	private int getImportBatchSize() {
		if (config.getPutBatchSize() == null) {
			if (config.isAllOrNew()) {
				return DEFAULT_BATCH_SIZE_CONTINUOUS;
			}
			return DEFAULT_BATCH_SIZE;
		}
		return config.getPutBatchSize();
	}

	private ITransfer getExport(Metaspace metaspace) throws Exception {
		SpaceInputStream in = new SpaceInputStream(metaspace, config);
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		config.setSpace(spaceDef.getName());
		if (config.getFields().isEmpty()) {
			populateConfig(spaceDef);
		}
		for (FieldConfig field : config.getFields()) {
			String fieldName = field.getFieldName();
			FieldDef fieldDef = spaceDef.getFieldDef(fieldName);
			if (fieldDef == null) {
				continue;
			}
			field.setFieldType(fieldDef.getType());
			field.setFieldNullable(fieldDef.isNullable());
			field.setFieldEncrypted(fieldDef.isEncrypted());
		}
		config.setKeys(spaceDef.getKeyDef().getFieldNames());
		return getExport(in, spaceDef);
	}

	private ITransfer getImport(Metaspace metaspace) throws Exception {
		IInputStream<Object[]> in = getInputStream();
		String spaceName = config.getSpace();
		SpaceDef spaceDef = metaspace.getSpaceDef(spaceName);
		if (spaceDef == null) {
			spaceDef = SpaceDef.create(spaceName);
			for (FieldConfig field : config.getFields()) {
				FieldDef fieldDef = FieldDef.create(field.getFieldName(),
						field.getFieldType());
				if (field.getFieldEncrypted() != null) {
					fieldDef.setEncrypted(field.getFieldEncrypted());
				}
				if (field.getFieldNullable() != null) {
					fieldDef.setNullable(field.getFieldNullable());
				}
				spaceDef.getFieldDefs().add(fieldDef);
			}
			spaceDef.setKey(config.getKeys().toArray(
					new String[config.getKeys().size()]));
			metaspace.defineSpace(spaceDef);
		} else {
			if (config.getFields().isEmpty()) {
				populateConfig(spaceDef);
			}
		}
		int batchSize = getImportBatchSize();
		List<Runnable> workers = new ArrayList<Runnable>();
		for (int index = 0; index < getWorkerCount(); index++) {
			IOperation operation = getOperation(metaspace);
			SpaceOutputStream out = new SpaceOutputStream(operation);
			IConverter<Object[], Tuple> converter = getImportConverter(spaceDef);
			workers.add(createWorker(in, converter, out, batchSize));
		}
		String name = getImportName();
		return new Transfer(name, in, workers);
	}

	private void populateConfig(SpaceDef spaceDef) {
		for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
			FieldConfig field = config.createFieldConfig();
			field.setFieldName(fieldDef.getName());
			config.getFields().add(field);
		}
	}

	protected String getImportName() {
		return config.getSpace();
	}

	protected int getExportBatchSize() {
		return DEFAULT_BATCH_SIZE;
	}

	private IConverter<Tuple, Object[]> getExportConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException {
		List<FieldConfig> fields = config.getFields();
		ITupleAccessor[] accessors = new ITupleAccessor[fields.size()];
		IConverter<?, ?>[] converters = new IConverter[fields.size()];
		for (int index = 0; index < fields.size(); index++) {
			FieldConfig column = fields.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(column.getFieldName());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(), fieldDef, column.getJavaType());
		}
		return new TupleToArrayConverter<Object>(accessors, converters,
				Object.class);
	}

	private IConverter<Object[], Tuple> getImportConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException {
		List<FieldConfig> fields = config.getFields();
		ITupleAccessor[] accessors = new ITupleAccessor[fields.size()];
		IConverter<?, ?>[] converters = new IConverter[fields.size()];
		for (int index = 0; index < fields.size(); index++) {
			FieldConfig field = fields.get(index);
			FieldDef fieldDef = spaceDef.getFieldDef(field.getFieldName());
			accessors[index] = AccessorFactory.create(fieldDef);
			converters[index] = converterFactory.getConverter(
					config.getAttributes(), field.getJavaType(), fieldDef);
		}
		return new ArrayToTupleConverter<Object>(accessors, converters);
	}

	protected abstract IInputStream<Object[]> getInputStream() throws Exception;

	protected abstract IOutputStream<Object[]> getOutputStream()
			throws Exception;

	private IOperation getOperation(Metaspace metaspace) throws ASException {
		Space space = getSpace(metaspace);
		long timeout = getWaitForReadyTimeout();
		boolean keepOpen = isSeeder();
		switch (getOperationType()) {
		case GET:
			return new GetOperation(space, timeout, keepOpen,
					GetOptions.create());
		case LOAD:
			return new LoadOperation(space, timeout, keepOpen);
		case NONE:
			return new NoOperation();
		case PARTIAL:
			return new PartialOperation(space, timeout, keepOpen, PutOptions
					.create().setForget(true));
		case PUT:
			return new PutOperation(space, timeout, keepOpen, PutOptions
					.create().setForget(true));
		case TAKE:
			return new TakeOperation(space, timeout, keepOpen, TakeOptions
					.create().setForget(true));
		}
		throw new RuntimeException("Invalid operation");
	}

	private boolean isSeeder() {
		return config.getDistributionRole() == DistributionRole.SEEDER;
	}

	private long getWaitForReadyTimeout() {
		if (config.getWaitForReadyTimeout() == null) {
			return DEFAULT_WAIT_FOR_READY_TIMEOUT;
		}
		return config.getWaitForReadyTimeout();
	}

	private OperationType getOperationType() {
		if (config.getOperation() == null) {
			return OperationType.PUT;
		}
		return config.getOperation();
	}

	private Space getSpace(Metaspace metaspace) throws ASException {
		String spaceName = config.getSpace();
		if (config.getDistributionRole() == null) {
			return metaspace.getSpace(spaceName);
		} else {
			return metaspace.getSpace(spaceName, config.getDistributionRole());
		}
	}

	@Override
	public void close() throws Exception {
		for (ITransfer transfer : transfers) {
			channel.closing(transfer);
			transfer.close();
			channel.closed(transfer);
		}
		if (isSeeder()) {
			return;
		}
		transfers.clear();
	}

	@Override
	public void stop() throws Exception {
		for (ITransfer transfer : transfers) {
			transfer.stop();
		}
	}

	protected int getWorkerCount() {
		if (config.getWorkerCount() == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return config.getWorkerCount();
	}

}