package com.tibco.as.io.cli;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tibco.as.io.IMetaspaceTransferListener;
import com.tibco.as.io.AbstractDestinationTransfer;

public class MetaspaceTransferMonitor implements IMetaspaceTransferListener {

	ExecutorService executor = Executors.newSingleThreadExecutor();

	@Override
	public void executing(AbstractDestinationTransfer transfer) {
		executor.execute(getConsole(transfer));
	}

	private Console getConsole(AbstractDestinationTransfer transfer) {
		return new Console(transfer.getName(), transfer.getInputStream());
	}

}
