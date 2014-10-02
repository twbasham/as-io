package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
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

public abstract class AbstractDestination<T> implements IDestination {

	private static final int DEFAULT_WORKER_COUNT = 1;

	private static final int DEFAULT_BATCH_SIZE = 1000;

	private static final int DEFAULT_BATCH_SIZE_CONTINUOUS = 1;

	private static final long DEFAULT_WAIT_FOR_READY_TIMEOUT = 30000;

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

	public AbstractDestination(DestinationConfig config) {
		this.config = config;
	}

	@Override
	public void open(Metaspace metaspace) throws Exception {
		transfers.add(getTransfer(metaspace));
		for (ITransfer transfer : transfers) {
			transfer.open();
		}
	}

	private ITransfer getTransfer(Metaspace metaspace) throws Exception {
		if (config.getDirection() == Direction.IMPORT) {
			return getImport(metaspace);
		}
		return getExport(metaspace);
	}

	private ITransfer getExport(Metaspace metaspace) throws Exception {
		SpaceInputStream in = new SpaceInputStream(metaspace, config);
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		config.setSpace(spaceDef.getName());
		if (config.getFields().isEmpty()) {
			for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
				FieldConfig field = config.createFieldConfig();
				field.setFieldName(fieldDef.getName());
				config.getFields().add(field);
			}
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
		IOutputStream<T> out = getOutputStream();
		Collection<IConverter<Tuple, T>> converters = new ArrayList<IConverter<Tuple, T>>();
		for (int index = 0; index < getWorkerCount(); index++) {
			converters.add(getExportConverter(spaceDef));
		}
		int batchSize = getExportBatchSize();
		return new Transfer<Tuple, T>(in, converters, out, batchSize);

	}

	private ITransfer getImport(Metaspace metaspace) throws Exception {
		IInputStream<T> in = getInputStream();
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
		}
		SpaceOutputStream out = new SpaceOutputStream(getOperation(metaspace));
		Collection<IConverter<T, Tuple>> converters = new ArrayList<IConverter<T, Tuple>>();
		for (int index = 0; index < getWorkerCount(); index++) {
			converters.add(getImportConverter(spaceDef));
		}
		return new Transfer<T, Tuple>(in, converters, out, getImportBatchSize());
	}

	protected abstract int getImportBatchSize();

	protected abstract IConverter<Tuple, T> getExportConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException;

	protected abstract IConverter<T, Tuple> getImportConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException;

	protected abstract IInputStream<T> getInputStream() throws Exception;

	protected abstract IOutputStream<T> getOutputStream() throws Exception;

	private IOperation getOperation(Metaspace metaspace) throws ASException {
		Space space = getSpace(metaspace);
		long timeout = getWaitForReadyTimeout();
		boolean keepOpen = isKeepSpaceOpen();
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

	private long getWaitForReadyTimeout() {
		if (config.getWaitForReadyTimeout() == null) {
			return DEFAULT_WAIT_FOR_READY_TIMEOUT;
		}
		return config.getWaitForReadyTimeout();
	}

	private boolean isKeepSpaceOpen() {
		if (config.getKeepSpaceOpen() == null) {
			return config.getDistributionRole() == DistributionRole.SEEDER;
		}
		return config.getKeepSpaceOpen();
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
			transfer.close();
		}
		transfers.clear();
	}

	@Override
	public void stop() throws Exception {
		for (ITransfer transfer : transfers) {
			transfer.stop();
		}
	}

	private int getExportBatchSize() {
		if (config.getPutBatchSize() == null) {
			if (config.isAllOrNew()) {
				return DEFAULT_BATCH_SIZE_CONTINUOUS;
			}
			return DEFAULT_BATCH_SIZE;
		}
		return config.getPutBatchSize();
	}

	protected int getWorkerCount() {
		if (config.getWorkerCount() == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return config.getWorkerCount();
	}

}