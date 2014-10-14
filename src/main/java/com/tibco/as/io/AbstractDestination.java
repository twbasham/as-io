package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tibco.as.convert.ConverterFactory;
import com.tibco.as.convert.IConverter;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;

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

	@Override
	public IInputStream getInputStream() {
		return in;
	}

	protected DestinationConfig getConfig() {
		return config;
	}

	protected IChannel getChannel() {
		return channel;
	}

	@Override
	public void open(Metaspace metaspace) throws Exception {
		in = getInputStream(createInputStream(metaspace));
		SpaceDef spaceDef = metaspace.getSpaceDef(config.getSpace());
		if (spaceDef != null) {
			config.setSpaceDef(spaceDef);
		}
		in.open();
		int workerCount = config.getWorkerCount();
		service = Executors.newFixedThreadPool(workerCount);
		for (int index = 0; index < workerCount; index++) {
			IOutputStream out = createOutputStream(metaspace);
			out.open();
			IConverter converter = converterFactory.getArrayConverter(config);
			Worker worker = new Worker(in, converter, out);
			workers.add(worker);
			service.execute(worker);
		}
		closed = false;
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
	public String getName() {
		return config.getSpace();
	}

}