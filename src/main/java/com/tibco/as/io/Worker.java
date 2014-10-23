package com.tibco.as.io;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.IConverter;
import com.tibco.as.log.LogFactory;

public class Worker implements Runnable {

	private Logger log = LogFactory.getLog(Worker.class);
	private IInputStream in;
	private IConverter converter;
	private IOutputStream out;

	public Worker(IInputStream in, IConverter converter, IOutputStream out) {
		this.in = in;
		this.converter = converter;
		this.out = out;
	}

	@Override
	public void run() {
		Object element;
		try {
			while ((element = in.read()) != null) {
				Object converted;
				try {
					converted = converter.convert(element);
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not convert", e);
					continue;
				}
				if (converted == null) {
					continue;
				}
				try {
					out.write(converted);
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not write", e);
				}
			}
		} catch (InterruptedException e) {
			log.log(Level.INFO, "Interrupted");
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not execute", e);
		}
	}

	public IInputStream getInputStream() {
		return in;
	}

	public IOutputStream getOutputStream() {
		return out;
	}

}
