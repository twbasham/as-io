package com.tibco.as.io.cli;

import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.IMetaspaceTransferListener;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.Transfer;
import com.tibco.as.io.TransferException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.Tuple;

public abstract class AbstractCommand implements IMetaspaceTransferListener {

	private static final String FIELD_BATCH_SIZE = "batchSize";

	private static final String FIELD_WORKER_COUNT = "workerCount";

	@Parameter(description = "Transfer output batch size", names = { "-batch_size" })
	private Integer batchSize;
	@Parameter(description = "Number of writer threads", names = { "-writer_thread_count" })
	private Integer workerCount;

	public void configure(Transfer transfer) {
		transfer.setBatchSize(batchSize);
		transfer.setWorkerCount(workerCount);
	}

	public void execute(Metaspace metaspace) {
		Collection<IMetaspaceTransfer> transfers = getMetaspaceTransfers(metaspace);
		for (IMetaspaceTransfer transfer : transfers) {
			transfer.addListener(this);
			try {
				transfer.execute();
			} catch (TransferException e) {
				String message = e.getLocalizedMessage();
				if (e.getCause() != null) {
					message += ": " + e.getCause().getLocalizedMessage();
				}
				System.err.println(message);
			}
		}
	}

	protected abstract Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace);

	@Override
	public void opening(Collection<ITransfer> transfers) {
		System.out.println(getExecutingMessage(transfers));
	}

	@Override
	public void executing(ITransfer transfer) {
		transfer.addListener(new AbstractTransferListener(transfer) {

			@Override
			protected String getOpenedMessage(ITransfer transfer) {
				return AbstractCommand.this.getOpenedMessage(transfer);
			}

			@Override
			protected String getClosedMessage(ITransfer transfer) {
				return AbstractCommand.this.getClosedMessage(transfer);
			}

		});
	}

	protected abstract String getExecutingMessage(
			Collection<ITransfer> transfers);

	protected abstract String getOpenedMessage(ITransfer transfer);

	protected abstract String getClosedMessage(ITransfer transfer);

	public void prepare() throws Exception {
	}

	protected void configure(Tuple context) {
		if (batchSize != null) {
			context.putInt(FIELD_BATCH_SIZE, batchSize);
		}
		if (workerCount != null) {
			context.putInt(FIELD_WORKER_COUNT, workerCount);
		}
	}

	protected void initialize(Space space, Tuple context) {
		batchSize = context.getInt(FIELD_BATCH_SIZE);
		workerCount = context.getInt(FIELD_WORKER_COUNT);
	}

}
