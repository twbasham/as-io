package com.tibco.as.io.cli;

import com.tibco.as.io.IDestination;

public class SimpleConsole extends AbstractConsole {

	private static final String FORMAT = "\r%1$-20s %2$,d";

	protected SimpleConsole(IDestination destination) {
		super(destination);
	}

	@Override
	protected void print(String name, long position) {
		System.out.printf(FORMAT, name, position);
	}

}
