package com.tibco.as.io.cli;

import com.tibco.as.io.IInputStream;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.ITransferListener;

public abstract class AbstractTransferListener implements ITransferListener {

	private static final char PROGRESS_DASH = '=';

	private static final char PROGRESS_ARROW = '-';

	private static final char PROGRESS_BLANK = ' ';

	private ITransfer transfer;

	public AbstractTransferListener(ITransfer transfer) {
		this.transfer = transfer;
	}

	@Override
	public void transferred(int count) {
		long size = transfer.size();
		long position = transfer.getInputStream().getPosition();
		if (size == IInputStream.UNKNOWN_SIZE) {
			printProgBar((int) position % 100, false);
		} else {
			printProgBar((int) ((double) (position - 1) / (double) size * 100),
					true);
		}
	}

	@Override
	public void opened() {
		System.out.println(getOpenedMessage(transfer));
	}

	protected abstract String getOpenedMessage(ITransfer transfer);

	@Override
	public void closed() {
		printProgBar(100, true);
		System.out.println();
		System.out.println(getClosedMessage(transfer));
	}

	protected abstract String getClosedMessage(ITransfer transfer);

	private void printProgBar(int percent, boolean showPercent) {
		int half = percent / 2;
		StringBuilder bar = new StringBuilder("[");
		for (int i = 0; i < 50; i++) {
			bar.append(getChar(i, half));
		}
		bar.append("]   ");
		if (showPercent) {
			bar.append(percent);
			bar.append("%     ");
		}
		System.out.print("\r" + bar.toString());
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
