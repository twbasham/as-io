package com.tibco.as.io.cli;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.tibco.as.convert.Blob;
import com.tibco.as.convert.Settings;
import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.MetaspaceTransfer;
import com.tibco.as.io.cli.converters.BlobConverter;
import com.tibco.as.io.cli.converters.LogLevelConverter;
import com.tibco.as.log.LogFactory;
import com.tibco.as.log.LogLevel;
import com.tibco.as.util.Member;

public abstract class AbstractApplication {

	private Logger log = LogFactory.getLog(AbstractApplication.class);

	@Parameter(names = { "-?", "-help" }, description = "Print this help message", help = true)
	private Boolean help;
	@Parameter(names = "-debug", converter = LogLevelConverter.class, validateWith = LogLevelConverter.class, description = "Log level (ERROR, WARNING, INFO, DEBUG or VERBOSE)")
	private LogLevel logLevel = LogLevel.INFO;
	@Parameter(names = "-log", description = "Log file path")
	private String logFile;
	@Parameter(names = "-log_debug", converter = LogLevelConverter.class, validateWith = LogLevelConverter.class, description = "File log level (ERROR, WARNING, INFO, DEBUG or VERBOSE)")
	private LogLevel logFileLevel = LogLevel.INFO;
	@Parameter(names = "-log_limit", description = "Max number of bytes to write to any log file")
	private Integer logFileLimit;
	@Parameter(names = "-log_count", description = "Number of log files to cycle through")
	private int logFileCount = 1;
	@Parameter(names = "-log_append", description = "Append logs onto any existing files")
	private boolean logFileAppend;
	@Parameter(names = "-metaspace", description = "Name of the metaspace that the application is to join")
	private String metaspaceName;
	@Parameter(names = "-member_name", description = "Unique member name")
	private String memberName;
	@Parameter(names = "-discovery", description = "URL to be used to discover the metaspace")
	private String discovery;
	@Parameter(names = "-listen", description = "URL for the application")
	private String listen;
	@Parameter(names = "-rx_buffer_size", description = "TCP buffer size for receiving data")
	private Long rxBufferSize;
	@Parameter(names = "-worker_thread_count", description = "Number of threads that can be used for program invocation")
	private Integer workerThreadCount;
	@Parameter(names = "-data_store", description = "Directory path for the shared-nothing persistence data store")
	private String dataStore;
	@Parameter(names = "-security_token", description = "Security token path")
	private String securityToken;
	@Parameter(names = "-identity_password", description = "Identity password")
	private String identityPassword;
	@Parameter(names = "-format_blob", description = "Blob format (\"base64\" or \"hex\")", converter = BlobConverter.class, validateWith = BlobConverter.class)
	private Blob blob;
	@Parameter(names = "-format_boolean_true", description = "Boolean format for 'true' value e.g. \"true\"")
	private String booleanTruePattern;
	@Parameter(names = "-format_boolean_false", description = "Boolean format for 'false' value e.g. \"false\"")
	private String booleanFalsePattern;
	@Parameter(names = "-format_datetime", description = "Date/time format e.g. \"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"")
	private String datePattern;
	@Parameter(names = "-format_number", description = "Number format e.g. \"###,###.###\"")
	private String numberPattern;
	@Parameter(names = "-time_zone", description = "Time zone ID e.g. \"GMT\"")
	private String timeZoneID;

	protected AbstractApplication() {
	}

	public void execute(String[] args) {
		JCommander jc = new JCommander(this);
		jc.setProgramName(getProgramName());
		addCommands(jc);
		try {
			jc.parse(args);
		} catch (ParameterException e) {
			System.err.println(MessageFormat.format(
					"Could not parse command: {0}", e.getLocalizedMessage()));
			return;
		}
		if (args.length == 0 || Boolean.TRUE.equals(help)) {
			jc.usage();
			return;
		}
		try {
			LogFactory.configure(logLevel, logFile, logFileLimit, logFileCount,
					logFileAppend, logFileLevel);
		} catch (Exception e) {
			System.err.println(MessageFormat.format(
					"Could not initialize logging: {0}",
					e.getLocalizedMessage()));
		}
		List<ICommand> commands = new ArrayList<ICommand>();
		String parsedCommand = jc.getParsedCommand();
		if (parsedCommand == null) {
			ICommand defaultCommand = getDefaultCommand();
			if (defaultCommand == null) {
				LogFactory.getLog(AbstractApplication.class).warning(
						"No command specified");
				jc.usage();
				return;
			}
			commands.add(defaultCommand);
		} else {
			JCommander commandJC = jc.getCommands().get(parsedCommand);
			for (Object command : commandJC.getObjects()) {
				commands.add((ICommand) command);
			}
		}
		AbstractChannel channel = getChannel();
		channel.setMetaspaceName(metaspaceName);
		Member member = new Member();
		member.setDataStore(dataStore);
		member.setDiscovery(discovery);
		member.setIdentityPassword(identityPassword);
		member.setListen(listen);
		member.setMemberName(memberName);
		member.setRxBufferSize(rxBufferSize);
		member.setSecurityTokenFile(securityToken);
		member.setWorkerThreadCount(workerThreadCount);
		channel.setMember(member);
		Settings settings = new Settings();
		settings.setBlob(blob);
		settings.setBooleanTruePattern(booleanTruePattern);
		settings.setBooleanFalsePattern(booleanFalsePattern);
		settings.setDatePattern(datePattern);
		settings.setNumberPattern(numberPattern);
		settings.setTimeZoneID(timeZoneID);
		channel.getDefaultDestination().setSettings(settings);
		try {
			channel.open();
			for (ICommand command : commands) {
				MetaspaceTransfer transfer;
				try {
					transfer = command.getTransfer(channel);
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not create transfer", e);
					continue;
				}
				transfer.addListener(new MetaspaceTransferMonitor());
				try {
					transfer.execute();
				} catch (InterruptedException e) {
					log.log(Level.INFO, "Transfer interrupted", e);
				}
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not open channel", e);
		} finally {
			try {
				channel.close();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not close channel", e);
			}
		}
	}

	protected abstract AbstractChannel getChannel();

	protected ICommand getDefaultCommand() {
		return null;
	}

	protected void addCommands(JCommander jc) {
	}

	protected abstract String getProgramName();

}
