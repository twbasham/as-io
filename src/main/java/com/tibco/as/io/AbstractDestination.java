package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.Field;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.log.LogFactory;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;

public abstract class AbstractDestination implements IDestination {

	private Logger log = LogFactory.getLog(AbstractDestination.class);
	private ConverterFactory converterFactory = new ConverterFactory();
	private AbstractChannel channel;
	private DestinationConfig config;
	private IInputStream in;
	private Collection<Worker> workers = new ArrayList<Worker>();
	private ExecutorService service;

	protected AbstractDestination(AbstractChannel channel,
			DestinationConfig config) {
		this.channel = channel;
		this.config = config;
	}

	protected AbstractChannel getChannel() {
		return channel;
	}

	@Override
	public void start() throws Exception {
		in = getInputStream(createInputStream());
		in.open();
		for (int index = 0; index < config.getWorkerCount(); index++) {
			IOutputStream out = getOutputStream();
			out.open();
			IConverter converter = getConverter();
			workers.add(new Worker(in, converter, out));
		}
		if (!config.isNoTransfer()) {
			service = Executors.newFixedThreadPool(workers.size());
			for (Worker worker : workers) {
				service.execute(worker);
			}
			service.shutdown();
		}
	}

	private IInputStream getInputStream(IInputStream in) {
		if (config.getLimit() == null) {
			return in;
		}
		return new LimitedInputStream(in, config.getLimit());
	}

	@Override
	public void awaitTermination() throws InterruptedException {
		if (service == null) {
			return;
		}
		while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
			// do nothing
		}
	}

	@Override
	public void stop() throws Exception {
		in.close();
		for (Worker worker : workers) {
			try {
				worker.getOutputStream().close();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not close output stream", e);
			}
		}
		if (config.getDistributionRole() == DistributionRole.SEEDER) {
			return;
		}
		workers.clear();
	}

	private IConverter getConverter() throws UnsupportedConversionException {
		Collection<Field> fields = config.getFields();
		for (Field field : fields) {
			field.getConversion().setDefaults(config.getConversion());
		}
		if (config.isImport()) {
			return converterFactory.getJavaConverter(fields);
		}
		return converterFactory.getTupleConverter(fields, getComponentType());
	}

	protected abstract Class<?> getComponentType();

	private IInputStream createInputStream() throws Exception {
		if (config.isImport()) {
			return getImportInputStream();
		}
		return new SpaceInputStream(channel.getMetaspace(), config);
	}

	private IOutputStream getOutputStream() throws Exception {
		if (config.isImport()) {
			Metaspace metaspace = channel.getMetaspace();
			int batchSize = config.getSpaceBatchSize();
			if (batchSize > 1) {
				return new BatchSpaceOutputStream(metaspace, config, batchSize);
			}
			return new SpaceOutputStream(metaspace, config);
		}
		return getExportOutputStream();
	}

	protected abstract IInputStream getImportInputStream() throws Exception;

	protected abstract IOutputStream getExportOutputStream() throws Exception;

	@Override
	public String getName() {
		return config.getSpace();
	}

	@Override
	public boolean hasCompleted() {
		if (service == null) {
			return true;
		}
		return service.isTerminated();
	}

	@Override
	public Long getPosition() {
		if (in == null) {
			return null;
		}
		return in.getPosition();
	}

	@Override
	public Long size() {
		if (in == null) {
			return null;
		}
		return in.size();
	}

}