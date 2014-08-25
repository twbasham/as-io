package com.tibco.as.io;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.io.IEvent.Severity;

public class EventManager {

	private static EventManager instance;

	private static boolean debug;

	private Collection<IEventListener> listeners = new ArrayList<IEventListener>();

	private EventManager() {
	}

	private static EventManager getInstance() {
		if (instance == null) {
			instance = new EventManager();
		}
		return instance;
	}

	public static void addListener(IEventListener listener) {
		getInstance().listeners.add(listener);
	}

	public static void error(String pattern, Object... arguments) {
		notify(Severity.ERROR, pattern, arguments);
	}

	public static void error(Throwable e, String pattern, Object... arguments) {
		notify(Severity.ERROR, e, pattern, arguments);
	}

	public static void error(Throwable e) {
		notify(new Event(Severity.ERROR, e));
	}

	public static void warn(Throwable e) {
		notify(new Event(Severity.WARN, e));
	}

	public static void debug(String pattern, Object... arguments) {
		if (debug) {
			notify(Severity.DEBUG, pattern, arguments);
		}
	}

	public static void info(String pattern, Object... arguments) {
		notify(Severity.INFO, pattern, arguments);
	}

	public static void warn(String pattern, Object... arguments) {
		notify(Severity.WARN, pattern, arguments);
	}

	public static void notify(Severity severity, String pattern,
			Object... arguments) {
		notify(createEvent(severity, pattern, arguments));
	}

	public static void notify(Severity severity, Throwable throwable,
			String pattern, Object... arguments) {
		notify(createEvent(severity, throwable, pattern, arguments));
	}

	private static void notify(IEvent event) {
		for (IEventListener listener : getInstance().listeners) {
			listener.onEvent(event);
		}
	}

	private static IEvent createEvent(Severity severity, String pattern,
			Object... arguments) {
		return new Event(severity, MessageFormat.format(pattern, arguments));
	}

	private static IEvent createEvent(Severity severity, Throwable throwable,
			String pattern, Object... arguments) {
		String message = MessageFormat.format(pattern, arguments);
		return new Event(severity, message, throwable);
	}

	public static boolean isDebugEnabled() {
		return debug;
	}

	public static void notify(Severity severity, Throwable throwable) {
		notify(new Event(severity, throwable));
	}

}
