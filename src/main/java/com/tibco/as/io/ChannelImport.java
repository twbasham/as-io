package com.tibco.as.io;

import java.util.Collection;

public class ChannelImport extends AbstractChannelTransfer {

	public ChannelImport(Channel channel) {
		super(channel);
	}

	@Override
	protected DestinationTransfer getTransfer(Destination destination)
			throws Exception {
		IInputStream inputStream = getInputStream(destination);
		IOutputStream outputStream = getOutputStream(destination);
		int workerCount = destination.getImportWorkerCount();
		return new DestinationTransfer(destination.getName(),
				workerCount, inputStream, outputStream);
	}

	private IOutputStream getOutputStream(Destination destination) {
		Integer batchSize = destination.getSpaceBatchSize();
		if (batchSize == null) {
			return new SpaceOutputStream(destination);
		}
		return new BatchSpaceOutputStream(destination, batchSize);
	}

	private IInputStream getInputStream(Destination destination) {
		IInputStream inputStream = destination.getInputStream();
		Long limit = destination.getImportLimit();
		if (limit == null) {
			return inputStream;
		}
		return new LimitedInputStream(inputStream, limit);
	}

	@Override
	protected Collection<Destination> getDestinations(Channel channel)
			throws Exception {
		return channel.getImportDestinations();
	}

}
