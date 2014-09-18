package com.tibco.as.io.cli;

import java.util.logging.Level;

public enum LogLevel {

	ERROR(Level.SEVERE), DEBUG(Level.FINE), INFO(Level.INFO), WARNING(
			Level.WARNING), VERBOSE(Level.FINEST);

	public Level level;

	private LogLevel(Level level) {
		this.level = level;
	}

	public Level getLevel() {
		return level;
	}

}