package com.tibco.as.io.cli;

import java.text.DecimalFormat;

import com.tibco.as.io.IInputStream;

public class ProgressBar {

	private DecimalFormat decimalFormat = new DecimalFormat("###,###");

	private static final char PROGRESS_DASH = '=';

	private static final char PROGRESS_ARROW = '-';

	private static final char PROGRESS_BLANK = ' ';

	private long size;

	public ProgressBar(long size) {
		this.size = size;
	}

	public void update(long position) {
		if (size == IInputStream.UNKNOWN_SIZE) {
			printProgBar(position, (int) position % 100, false);
		} else {
			printProgBar(position, (int) ((double) (position - 1)
					/ (double) size * 100), true);
		}
	}

	private void printProgBar(long position, int percent, boolean showPercent) {
		int half = percent / 2;
		StringBuilder bar = new StringBuilder("\r[");
		for (int i = 0; i < 50; i++) {
			bar.append(getChar(i, half));
		}
		bar.append("] ");
		if (showPercent) {
			bar.append(percent);
			bar.append("% ");
		}
		bar.append(decimalFormat.format(position - 1));
		System.out.print(bar.toString());
	}

	private char getChar(int index, int half) {
		if (index < half) {
			return PROGRESS_DASH;
		}
		if (index == half) {
			return PROGRESS_ARROW;
		}
		return PROGRESS_BLANK;
	}

}
