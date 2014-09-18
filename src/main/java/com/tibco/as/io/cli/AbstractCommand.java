package com.tibco.as.io.cli;

import java.util.Collection;
import java.util.logging.Logger;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.IMetaspaceTransferListener;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.AbstractTransfer;
import com.tibco.as.io.TransferException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.Tuple;

public abstract class AbstractCommand implements IMetaspaceTransferListener {

	private static final String FIELD_BATCH_SIZE = "batchSize";

	private static final String FIELD_WORKER_COUNT = "workerCount";

	private Logger log = Logger.getLogger(AbstractCommand.class.getName());

	@Parameter(description = "Transfer output batch size", names = { "-batch_size" })
	private Integer batchSize;
	@Parameter(description = "Number of writer threads", names = { "-writer_thread_count" })
	private Integer workerCount;

	public void configure(AbstractTransfer transfer) {
		transfer.setBatchSize(batchSize);
		transfer.setWorkerCount(workerCount);
	}

	public void execute(Metaspace metaspace) {
		Collection<IMetaspaceTransfer> transfers;
		try {
			transfers = getMetaspaceTransfers(metaspace);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return;
		}
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
			Metaspace metaspace) throws Exception;

	@Override
	public void opening(Collection<ITransfer> transfers) {
		log.info(getExecutingMessage(transfers));
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
