package com.tibco.as.io;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;

public class Worker implements Runnable {

	private Logger log = LogFactory.getLog(Worker.class);
	private IInputStream in;
	private IOutputStream out;

	public Worker(IInputStream in, IOutputStream out) {
		this.in = in;
		this.out = out;
	}

	@Override
	public void run() {
		try {
			in.open();
			out.open();
			Object element;
			try {
				while ((element = in.read()) != null) {
					try {
						out.write(element);
					} catch (Exception e) {
						log.log(Level.SEVERE, "Could not write", e);
					}
				}
			} catch (InterruptedException e) {
				log.log(Level.INFO, "Interrupted");
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not execute", e);
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not open output", e);
		} finally {
			try {
				out.close();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not close output", e);
			} finally {
				try {
					in.close();
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not close input", e);
				}
			}
		}
	}

	public IInputStream getInputStream() {
		return in;
	}

	public IOutputStream getOutputStream() {
		return out;
	}

}
