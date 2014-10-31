package com.tibco.as.io.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.DestinationTransfer;
import com.tibco.as.log.LogFactory;

public class Console implements Runnable {

	private static final String FORMAT = "\r%1$-20s %2$,d";
	private static final int WIDTH = 40;

	private Logger log = LogFactory.getLog(Console.class);
	private DestinationTransfer transfer;

	protected Console(DestinationTransfer transfer) {
		this.transfer = transfer;
	}

	@Override
	public void run() {
		while (!transfer.getInputStream().isClosed()) {
			print();
			sleep();
		}
		print();
		System.out.println();
	}

	private void sleep() {
		for (int index = 0; index < 3 && !transfer.getInputStream().isClosed(); index++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.log(Level.FINE, "Progress bar interruped", e);
			}
		}
	}

	protected void print() {
		String name = transfer.getName();
		Long size = transfer.size();
		Long position = transfer.getPosition();
		if (size == null) {
			System.out.printf(FORMAT, name, position == null ? 0 : null);
		} else {
			StringBuilder bar = new StringBuilder("\r%1$-20s [");
			long progress = getPercent(size, position) * WIDTH / 100;
			for (int i = 0; i < WIDTH; i++) {
				if (i < progress) {
					bar.append('=');
				} else if (i == progress) {
					bar.append('>');
				} else {
					bar.append(' ');
				}
			}
			bar.append("] ");
			bar.append("%2$,d/%3$,d");
			System.out.printf(bar.toString(), name, position, size);
		}
	}

	private long getPercent(long size, Long position) {
		if (size == 0) {
			return 100;
		}
		if (position == null) {
			return 0;
		}
		return position * 100 / size;
	}

}
