package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;

public abstract class AbstractDestination implements IDestination {

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

	@Override
	public void start() throws Exception {
		Metaspace metaspace = channel.getMetaspace();
		in = getInputStream(createInputStream(metaspace));
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		if (spaceDef != null) {
			config.setSpaceDef(spaceDef);
		}
		in.open();
		for (int index = 0; index < config.getWorkerCount(); index++) {
			IOutputStream out = createOutputStream(metaspace);
			workers.add(new Worker(in, createConverter(), out));
		}
		if (!config.isNoTransfer()) {
			service = Executors.newFixedThreadPool(workers.size());
			for (Worker worker : workers) {
				worker.open();
				service.execute(worker);
			}
			service.shutdown();
		}
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
			worker.close();
		}
		if (config.getDistributionRole() == DistributionRole.SEEDER) {
			return;
		}
		workers.clear();
	}

	private IConverter createConverter() throws UnsupportedConversionException {
		if (config.isImport()) {
			return converterFactory.getJavaConverter(config);
		}
		return converterFactory.getTupleConverter(config);
	}

	private IInputStream getInputStream(IInputStream in) {
		if (config.getLimit() == null) {
			return in;
		}
		return new LimitedInputStream(in, config.getLimit());
	}

	private IInputStream createInputStream(Metaspace metaspace)
			throws Exception {
		if (config.isImport()) {
			return createInputStream();
		}
		return new SpaceInputStream(metaspace, config);
	}

	private IOutputStream createOutputStream(Metaspace metaspace)
			throws Exception {
		if (config.isImport()) {
			int batchSize = config.getSpaceBatchSize();
			if (batchSize > 1) {
				return new BatchSpaceOutputStream(metaspace, config, batchSize);
			}
			return new SpaceOutputStream(metaspace, config);
		}
		return createOutputStream();
	}

	protected abstract IInputStream createInputStream() throws Exception;

	protected abstract IOutputStream createOutputStream() throws Exception;

	@Override
	public String getName() {
		return config.getSpace();
	}

	@Override
	public IInputStream getInputStream() {
		return in;
	}

	@Override
	public boolean hasCompleted() {
		if (service == null) {
			return true;
		}
		return service.isTerminated();
	}

}