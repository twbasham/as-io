package com.tibco.as.io.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.IDestination;
import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;

public abstract class AbstractConsole implements Runnable {

	protected static final int WIDTH = 40;

	private Logger log = LogFactory.getLog(AbstractConsole.class);
	private IDestination destination;

	protected AbstractConsole(IDestination destination) {
		this.destination = destination;
	}

	@Override
	public void run() {
		IInputStream in = destination.getInputStream();
		while (!isFinished()) {
			print(destination.getName(), in.getPosition());
			sleep();
		}
		print(destination.getName(), in.getPosition());
	}

	private void sleep() {
		for (int index = 0; index < 3; index++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.log(Level.FINE, "Progress bar interruped", e);
			}
			if (isFinished()) {
				return;
			}
		}
	}

	private boolean isFinished() {
		return destination.hasCompleted();
	}

	protected abstract void print(String name, long position);

}