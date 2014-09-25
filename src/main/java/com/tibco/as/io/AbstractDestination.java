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

	private IChannel channel;
	private DestinationConfig config;
	private ITransfer transfer;

	protected AbstractDestination(IChannel channel, DestinationConfig config) {
		this.channel = channel;
		this.config = config;
	}

	protected IChannel getChannel() {
		return channel;
	}

	protected SpaceDef getSpaceDef() throws ASException {
		return channel.getMetaspace().getSpaceDef(config.getSpaceName());
	}

	public AbstractDestination(DestinationConfig config) {
		this.config = config;
	}

	@Override
	public void open() throws Exception {
		transfer = getTransfer();
		transfer.open();
	}

	private ITransfer getTransfer() throws Exception {
		if (config.getDirection() == Direction.IMPORT) {
			return getImport();
		}
		return getExport();
	}

	private ITransfer getExport() throws Exception {
		SpaceDef spaceDef = getSpaceDef();
		SpaceInputStream in = getSpaceInputStream();
		IOutputStream<T> out = getOutputStream(config, spaceDef);
		Collection<IConverter<Tuple, T>> converters = new ArrayList<IConverter<Tuple, T>>();
		for (int index = 0; index < getWorkerCount(); index++) {
			converters.add(getExportConverter(config, spaceDef));
		}
		return new Transfer<Tuple, T>(in, converters, out, getBatchSize());

	}

	private ITransfer getImport() throws Exception {
		IInputStream<T> in = getInputStream(config);
		String spaceName = config.getSpaceName();
		SpaceDef spaceDef = channel.getMetaspace().getSpaceDef(spaceName);
		if (spaceDef == null) {
			spaceDef = SpaceDef.create(spaceName);
			populateSpaceDef(spaceDef, config);
			channel.getMetaspace().defineSpace(spaceDef);
		}
		SpaceOutputStream out = getSpaceOutputStream();
		Collection<IConverter<T, Tuple>> converters = new ArrayList<IConverter<T, Tuple>>();
		for (int index = 0; index < getWorkerCount(); index++) {
			converters.add(getImportConverter(config, spaceDef));
		}
		return new Transfer<T, Tuple>(in, converters, out, getBatchSize());
	}

	protected abstract void populateSpaceDef(SpaceDef spaceDef,
			DestinationConfig config) throws Exception;

	protected abstract IConverter<Tuple, T> getExportConverter(
			DestinationConfig config, SpaceDef spaceDef)
			throws UnsupportedConversionException;

	protected abstract IConverter<T, Tuple> getImportConverter(
			DestinationConfig config, SpaceDef spaceDef)
			throws UnsupportedConversionException;

	protected abstract IInputStream<T> getInputStream(DestinationConfig config)
			throws Exception;

	protected abstract IOutputStream<T> getOutputStream(
			DestinationConfig config, SpaceDef spaceDef) throws Exception;

	private SpaceOutputStream getSpaceOutputStream() throws ASException {
		return new SpaceOutputStream(getOperation());
	}

	private IOperation getOperation() throws ASException {
		Space space = getSpace();
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
		if (config.isKeepSpaceOpen() == null) {
			return config.getDistributionRole() == DistributionRole.SEEDER;
		}
		return config.isKeepSpaceOpen();
	}

	private OperationType getOperationType() {
		if (config.getOperation() == null) {
			return OperationType.PUT;
		}
		return config.getOperation();
	}

	private Space getSpace() throws ASException {
		Metaspace metaspace = channel.getMetaspace();
		String spaceName = config.getSpaceName();
		if (config.getDistributionRole() == null) {
			return metaspace.getSpace(spaceName);
		} else {
			return metaspace.getSpace(spaceName, config.getDistributionRole());
		}

	}

	private SpaceInputStream getSpaceInputStream() {
		return new SpaceInputStream(channel.getMetaspace(), config);
	}

	@Override
	public void close() throws Exception {
		if (transfer == null) {
			return;
		}
		transfer.close();
		transfer = null;
	}

	@Override
	public void stop() throws Exception {
		if (transfer == null) {
			return;
		}
		transfer.stop();
	}

	private int getBatchSize() {
		if (config.getBatchSize() == null) {
			if (config.isAllOrNew()) {
				return DEFAULT_BATCH_SIZE_CONTINUOUS;
			}
			return DEFAULT_BATCH_SIZE;
		}
		return config.getBatchSize();
	}

	protected int getWorkerCount() {
		if (config.getWorkerCount() == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return config.getWorkerCount();
	}

}