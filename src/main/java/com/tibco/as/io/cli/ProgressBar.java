package com.tibco.as.io.cli;

import java.text.DecimalFormat;

import com.tibco.as.io.IInputStream;

public class ProgressBar {

	private DecimalFormat decimalFormat = new DecimalFormat("###,###");

	private long size;

	public ProgressBar(long size) {
		this.size = size;
	}

	public void update(long position) {
		if (size == IInputStream.UNKNOWN_SIZE) {
			printProgBar(position, (int) position % 100);
		} else {
			printProgBar(position, (int) ((double) (position - 1)
					/ (double) size * 100));
		}
	}

	private void printProgBar(long position, int percent) {
		int half = percent / 2;
		StringBuilder bar = new StringBuilder("\r[");
		for (int i = 0; i < 50; i++) {
			bar.append(getChar(i, half));
		}
		bar.append("] ");
		bar.append(decimalFormat.format(position - 1));
		if (size != IInputStream.UNKNOWN_SIZE) {
			bar.append('/');
			bar.append(decimalFormat.format(size));
		}
		System.out.print(bar.toString());
	}

	private char getChar(int index, int half) {
		if (index < half) {
			return '=';
		}
		if (index == half) {
			return '>';
		}
		return ' ';
	}

}
