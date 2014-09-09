package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.tibco.as.convert.IConverter;
import com.tibco.as.convert.UnsupportedConversionException;
import com.tibco.as.io.transfer.AbstractWorker;
import com.tibco.as.io.transfer.BatchWorker;
import com.tibco.as.io.transfer.Executor;
import com.tibco.as.io.transfer.SimpleWorker;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;

public abstract class AbstractMetaspaceTransfer<T, U> implements IMetaspaceTransfer {

	private static final int DEFAULT_BATCH_SIZE = 1000;

	private static final int DEFAULT_WORKER_COUNT = 1;

	private Collection<IMetaspaceTransferListener> listeners = new ArrayList<IMetaspaceTransferListener>();

	private Metaspace metaspace;

	private boolean stopped;

	private Collection<AbstractTransfer> transfers = new ArrayList<AbstractTransfer>();

	private Collection<ITransfer> executors = new ArrayList<ITransfer>();

	private IInputStream<T> inputStream;

	private IOutputStream<U> outputStream;

	private AbstractTransfer defaultTransfer = createTransfer();

	@Override
	public void setDefaultTransfer(AbstractTransfer transfer) {
		this.defaultTransfer = transfer;
	}

	public AbstractTransfer getDefaultTransfer() {
		return defaultTransfer;
	}

	protected abstract AbstractTransfer createTransfer();

	public AbstractMetaspaceTransfer(Metaspace metaspace) {
		this.metaspace = metaspace;
	}

	public void setInputStream(IInputStream<T> inputStream) {
		this.inputStream = inputStream;
	}

	public void setOutputStream(IOutputStream<U> outputStream) {
		this.outputStream = outputStream;
	}

	@Override
	public void addListener(IMetaspaceTransferListener listener) {
		listeners.add(listener);
	}

	@Override
	public void execute() throws TransferException {
		this.stopped = false;
		if (transfers.isEmpty()) {
			transfers.addAll(getTransfers(metaspace));
		}
		for (AbstractTransfer transfer : transfers) {
			SpaceDef spaceDef;
			try {
				spaceDef = getSpaceDef(transfer);
			} catch (Exception e) {
				throw new TransferException("Could not get space def", e);
			}
			IInputStream<T> in = getInputStream(transfer, spaceDef);
			if (transfer.getLimit() != null) {
				in = new LimitedInputStream<T>(in, transfer.getLimit());
			}
			IOutputStream<U> out = getOutputStream(transfer, spaceDef);
			int batchSize = getBatchSize(transfer);
			int workerCount = getWorkerCount(transfer);
			int queueCapacity = getQueueCapacity(transfer, batchSize,
					workerCount);
			if (batchSize * workerCount > queueCapacity) {
				throw new TransferException("Queue capacity too low");
			}
			BlockingQueue<T> queue = new ArrayBlockingQueue<T>(queueCapacity);
			Executor<T, U> executor = new Executor<T, U>(queue, in, out);
			for (int index = 0; index < workerCount; index++) {
				IConverter<T, U> converter;
				try {
					converter = getConverter(transfer, spaceDef);
				} catch (UnsupportedConversionException e) {
					throw new TransferException("Unsupported conversion", e);
				}
				IWorker worker = createWorker(executor, batchSize, converter);
				executor.addWorker(worker);
			}
			executors.add(executor);
		}
		for (IMetaspaceTransferListener listener : listeners) {
			listener.opening(executors);
		}
		for (ITransfer transfer : executors) {
			transfer.open();
		}
		if (isParallelTransfers()) {
			for (ITransfer transfer : executors) {
				for (IMetaspaceTransferListener listener : listeners) {
					listener.executing(transfer);
				}
				new Thread(transfer).start();
			}
			for (ITransfer transfer : executors) {
				try {
					while (!transfer.awaitTermination(100,
							TimeUnit.MILLISECONDS)) {
						// do nothing
					}
				} catch (InterruptedException e) {
					throw new TransferException(
							"Interrupted while waiting for transfer termination",
							e);
				}
			}
		} else {
			for (ITransfer transfer : executors) {
				for (IMetaspaceTransferListener listener : listeners) {
					listener.executing(transfer);
				}
				transfer.execute();
			}
		}
		for (ITransfer transfer : executors) {
			transfer.close();
		}
	}

	protected boolean isParallelTransfers() {
		return false;
	}

	public Collection<ITransfer> getExecutors() {
		return executors;
	}

	private int getQueueCapacity(AbstractTransfer transfer, int batchSize,
			int workerCount) {
		Integer queueCapacity = transfer.getQueueCapacity();
		if (queueCapacity == null || queueCapacity == 0) {
			return batchSize * (workerCount + 1);
		}
		return queueCapacity;
	}

	protected int getBatchSize(AbstractTransfer transfer) {
		Integer batchSize = transfer.getBatchSize();
		if (batchSize == null || batchSize == 0) {
			return DEFAULT_BATCH_SIZE;
		}
		return batchSize;
	}

	protected Integer getWorkerCount(AbstractTransfer transfer) {
		Integer workerCount = transfer.getWorkerCount();
		if (workerCount == null || workerCount == 0) {
			return DEFAULT_WORKER_COUNT;
		}
		return workerCount;
	}

	protected abstract IConverter<T, U> getConverter(AbstractTransfer transfer,
			SpaceDef spaceDef) throws UnsupportedConversionException;

	private AbstractWorker<T, U> createWorker(Executor<T, U> executor,
			int batchSize, IConverter<T, U> converter) {
		if (batchSize > 1) {
			return new BatchWorker<T, U>(executor, converter, batchSize);
		}
		return new SimpleWorker<T, U>(executor, converter);
	}

	private IInputStream<T> getInputStream(AbstractTransfer transfer, SpaceDef spaceDef)
			throws TransferException {
		if (inputStream == null) {
			return getInputStream(metaspace, transfer, spaceDef);
		}
		return inputStream;
	}

	private IOutputStream<U> getOutputStream(AbstractTransfer transfer,
			SpaceDef spaceDef) throws TransferException {
		if (outputStream == null) {
			return getOutputStream(metaspace, transfer, spaceDef);
		}
		return outputStream;
	}

	protected abstract Collection<AbstractTransfer> getTransfers(Metaspace metaspace)
			throws TransferException;

	public SpaceDef getSpaceDef(AbstractTransfer transfer) throws Exception {
		return getSpaceDef(metaspace, transfer);
	}

	protected abstract SpaceDef getSpaceDef(Metaspace metaspace,
			AbstractTransfer transfer) throws Exception;

	protected abstract IInputStream<T> getInputStream(Metaspace metaspace,
			AbstractTransfer transfer, SpaceDef spaceDef) throws TransferException;

	protected abstract IOutputStream<U> getOutputStream(Metaspace metaspace,
			AbstractTransfer transfer, SpaceDef spaceDef) throws TransferException;

	@Override
	public void stop() throws Exception {
		for (ITransfer executor : executors) {
			executor.stop();
		}
		this.stopped = true;
	}

	@Override
	public boolean isStopped() {
		return stopped;
	}

	@Override
	public void addTransfer(AbstractTransfer transfer) {
		transfers.add(transfer);
	}

	@Override
	public Metaspace getMetaspace() {
		return metaspace;
	}

}
