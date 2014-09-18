package com.tibco.as.io.cli;

import java.text.DecimalFormat;
import java.util.logging.Logger;

import com.tibco.as.io.IInputStream;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.ITransferListener;

public abstract class AbstractTransferListener implements ITransferListener {

	private DecimalFormat decimalFormat = new DecimalFormat("###,###");

	private static final char PROGRESS_DASH = '=';

	private static final char PROGRESS_ARROW = '-';

	private static final char PROGRESS_BLANK = ' ';

	private Logger log = Logger.getLogger(AbstractTransferListener.class
			.getName());

	private ITransfer transfer;

	public AbstractTransferListener(ITransfer transfer) {
		this.transfer = transfer;
	}

	@Override
	public void transferred(int count) {
		long position = getPosition();
		long size = transfer.size();
		if (size == IInputStream.UNKNOWN_SIZE) {
			printProgBar(position, (int) position % 100, false);
		} else {
			printProgBar(position, (int) ((double) (position - 1)
					/ (double) size * 100), true);
		}
	}

	private long getPosition() {
		return transfer.getInputStream().getPosition();
	}

	@Override
	public void opened() {
		log.info(getOpenedMessage(transfer));
	}

	protected abstract String getOpenedMessage(ITransfer transfer);

	@Override
	public void closed() {
		printProgBar(getPosition(), 100, true);
		System.out.println();
		log.info(getClosedMessage(transfer));
	}

	protected abstract String getClosedMessage(ITransfer transfer);

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
