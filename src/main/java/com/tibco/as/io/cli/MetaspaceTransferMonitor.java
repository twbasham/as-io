package com.tibco.as.io.cli;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tibco.as.io.IMetaspaceTransferListener;
import com.tibco.as.io.AbstractTransfer;

public class MetaspaceTransferMonitor implements IMetaspaceTransferListener {

	ExecutorService executor = Executors.newSingleThreadExecutor();

	@Override
	public void executing(AbstractTransfer transfer) {
		executor.execute(getConsole(transfer));
	}

	private Console getConsole(AbstractTransfer transfer) {
		return new Console(transfer.getName(), transfer.getInputStream());
	}

}
