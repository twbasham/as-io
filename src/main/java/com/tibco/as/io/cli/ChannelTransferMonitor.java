package com.tibco.as.io.cli;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tibco.as.io.IChannelTransferListener;
import com.tibco.as.io.IDestinationTransfer;

public class ChannelTransferMonitor implements IChannelTransferListener {

	ExecutorService executor = Executors.newSingleThreadExecutor();

	@Override
	public void executing(IDestinationTransfer transfer) {
		executor.execute(getConsole(transfer));
	}

	private Console getConsole(IDestinationTransfer transfer) {
		return new Console(transfer);
	}

}
