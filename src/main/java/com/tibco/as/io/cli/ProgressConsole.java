package com.tibco.as.io.cli;

import com.tibco.as.io.IDestination;

public class ProgressConsole extends AbstractConsole {

	private long size;

	protected ProgressConsole(IDestination destination, long size) {
		super(destination);
		this.size = size;
	}

	@Override
	protected void print(String name, long position) {
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

	private long getPercent(long size, long position) {
		if (size == 0) {
			return 100;
		}
		return position * 100 / size;
	}

}
