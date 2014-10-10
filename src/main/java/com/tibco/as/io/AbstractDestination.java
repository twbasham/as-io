package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.accessors.AccessorFactory;
import com.tibco.as.accessors.ITupleAccessor;
import com.tibco.as.convert.Attributes;
import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.convert.array.ArrayToTupleConverter;
import com.tibco.as.convert.array.TupleToArrayConverter;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.FieldDef;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;

public abstract class AbstractDestination implements IDestination {

	private static final int DEFAULT_WORKER_COUNT = 1;

	private Logger log = LogFactory.getLog(AbstractDestination.class);
	private ConverterFactory converterFactory = new ConverterFactory();
	private AbstractChannel channel;
	private DestinationConfig config;
	private Collection<ITransfer> transfers = new ArrayList<ITransfer>();
	private boolean closed;

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
		closed = false;
		ITransfer transfer = getTransfer(metaspace);
		channel.opening(transfer);
		transfer.open();
		channel.opened(transfer);
		transfers.add(transfer);
	}

	private ITransfer getTransfer(Metaspace metaspace) throws Exception {
		String name = getTransferName();
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		if (spaceDef != null) {
			config.setSpace(spaceDef.getName());
			config.setKeys(spaceDef.getKeyDef().getFieldNames());
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
					log.log(Level.WARNING,
							"No field named ''{0}'' in space ''{0}''",
							new Object[] { fieldName, spaceDef.getName() });
				} else {
					field.setFieldType(fieldDef.getType());
					field.setFieldNullable(fieldDef.isNullable());
					field.setFieldEncrypted(fieldDef.isEncrypted());
				}
			}
			config.setKeys(spaceDef.getKeyDef().getFieldNames());
		}
		IInputStream in = getInputStream(metaspace);
		if (spaceDef == null) {
			spaceDef = SpaceDef.create(config.getSpace());
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
		Collection<Worker> workers = new ArrayList<Worker>();
		for (int index = 0; index < getWorkerCount(); index++) {
			IOutputStream out = getOutputStream(metaspace);
			IConverter converter = getConverter(spaceDef);
			workers.add(new Worker(in, converter, out));
		}
		return new Transfer(name, in, workers);
	}

	private String getTransferName() {
		if (isImport()) {
			return getImportName();
		}
		return getExportName();
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

	protected String getExportName() {
		return config.getSpace();
	}

	protected String getImportName() {
		return config.getSpace();
	}

	private IConverter getConverter(SpaceDef spaceDef)
			throws UnsupportedConversionException {
		Collection<ITupleAccessor> accessors = new ArrayList<ITupleAccessor>();
		Collection<IConverter> converters = new ArrayList<IConverter>();
		for (FieldConfig field : config.getFields()) {
			FieldDef fieldDef = spaceDef.getFieldDef(field.getFieldName());
			accessors.add(AccessorFactory.create(fieldDef));
			converters.add(getConverter(fieldDef, field.getJavaType()));
		}
		return getConverter(accessors, converters);
	}

	private IConverter getConverter(Collection<ITupleAccessor> accessors,
			Collection<IConverter> converters) {
		ITupleAccessor[] accessorArray = accessors
				.toArray(new ITupleAccessor[accessors.size()]);
		IConverter[] converterArray = converters
				.toArray(new IConverter[converters.size()]);
		if (isImport()) {
			return new ArrayToTupleConverter(accessorArray, converterArray);
		}
		return new TupleToArrayConverter(accessorArray, converterArray);
	}

	private IConverter getConverter(FieldDef fieldDef, Class<?> type)
			throws UnsupportedConversionException {
		Attributes attributes = config.getAttributes().getAttributes(
				fieldDef.getName());
		Class<?> from = isImport() ? type : ConverterFactory.getType(fieldDef);
		Class<?> to = isImport() ? ConverterFactory.getType(fieldDef) : type;
		return converterFactory.getConverter(attributes, from, to);
	}

	private boolean isImport() {
		return config.getDirection() == Direction.IMPORT;
	}

	protected abstract IInputStream getInputStream() throws Exception;

	protected abstract IOutputStream getOutputStream() throws Exception;

	@Override
	public void close() throws Exception {
		for (ITransfer transfer : transfers) {
			channel.closing(transfer);
			transfer.close();
			channel.closed(transfer);
		}
		closed = true;
		if (config.getDistributionRole() == DistributionRole.SEEDER) {
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

	@Override
	public boolean isClosed() {
		return closed;
	}

	protected int getWorkerCount() {
		if (config.getWorkerCount() == null) {
			return DEFAULT_WORKER_COUNT;
		}
		return config.getWorkerCount();
	}

}