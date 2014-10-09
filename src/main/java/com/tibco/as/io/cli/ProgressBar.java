package com.tibco.as.io.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.IInputStream;
import com.tibco.as.io.ITransfer;
import com.tibco.as.log.LogFactory;

public class ProgressBar implements Runnable {

	private static final int WIDTH = 40;

	private Logger log = LogFactory.getLog(ProgressBar.class);
	private ITransfer transfer;
	private boolean stopped = false;
	private int unknownProgress = 0;

	public ProgressBar(ITransfer transfer) {
		this.transfer = transfer;
	}

	@Override
	public void run() {
		long size = transfer.size();
		while (!stopped) {
			long position = transfer.getPosition();
			if (size == IInputStream.UNKNOWN_SIZE) {
				printProgBar(size, position, unknownProgress++ % 100);
			} else {
				printProgBar(size, position, getPercent(size, position));
			}
			sleep();
		}
		printProgBar(size, transfer.getPosition(), 100);
		System.out.println();
	}

	private long getPercent(long size, long position) {
		return position * 100 / size;
	}

	private void sleep() {
		for (int index = 0; index < 3; index++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.log(Level.FINE, "Progress bar interruped", e);
			}
			if (stopped) {
				return;
			}
		}
	}

	private void printProgBar(long size, long position, long percent) {
		StringBuilder bar = new StringBuilder("\r%1$-20s [");
		long progress = percent * WIDTH / 100;
		for (int i = 0; i < WIDTH; i++) {
			if (i < progress) {
				bar.append('=');
			} else if (i == progress) {
				bar.append('>');
			} else {
				bar.append(' ');
			}
		}
		bar.append("] %2$d%% %3$,d");
		if (size != IInputStream.UNKNOWN_SIZE) {
			bar.append("/%4$,d");
		}
		System.out.printf(bar.toString(), transfer.getName(), percent,
				position, size);
	}

	public void stop() {
		stopped = true;
	}

}
