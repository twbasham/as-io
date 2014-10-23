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
import com.tibco.as.convert.ConversionConfig;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.cli.converters.BlobConverter;
import com.tibco.as.io.cli.converters.LogLevelConverter;
import com.tibco.as.log.LogFactory;
import com.tibco.as.log.LogLevel;
import com.tibco.as.util.Member;

public abstract class AbstractApplication {

	private Logger log = LogFactory.getLog(AbstractApplication.class);

	@Parameter(names = { "-?", "-help" }, description = "Print this help message", help = true)
	private Boolean help;
	@Parameter(names = "-log_level", converter = LogLevelConverter.class, validateWith = LogLevelConverter.class, description = "Log level (ERROR, WARNING, INFO, DEBUG or VERBOSE)")
	private LogLevel logLevel = LogLevel.INFO;
	@Parameter(names = "-log_file", description = "Write logs to file")
	private boolean logFile;
	@Parameter(names = "-log_file_pattern", description = "Log file name pattern")
	private String logFilePattern;
	@Parameter(names = "-log_file_limit", description = "Approximate maximum amount to write (in bytes) to any log file")
	private Integer logFileLimit;
	@Parameter(names = "-log_file_count", description = "Number of log files to cycle through")
	private int logFileCount = 1;
	@Parameter(names = "-log_file_append", description = "Append logs onto any existing files")
	private boolean logFileAppend;
	@Parameter(names = "-metaspace", description = "Metaspace name")
	private String metaspaceName;
	@Parameter(names = "-member_name", description = "Member name")
	private String memberName;
	@Parameter(names = "-discovery", description = "Discovery URL")
	private String discovery;
	@Parameter(names = "-listen", description = "Listen URL")
	private String listen;
	@Parameter(names = "-rx_buffer_size", description = "Receive buffer size")
	private Long rxBufferSize;
	@Parameter(names = "-worker_thread_count", description = "Worker thread count")
	private Integer workerThreadCount;
	@Parameter(names = "-data_store", description = "Directory path for data store")
	private String dataStore;
	// @Parameter(names = "-no_exit" , description =
	// "Do not shut down after application execution")
	// private boolean noExit;
	@Parameter(names = "-security_token", description = "Security token path")
	private String securityToken;
	@Parameter(names = "-identity_password", description = "Identity password")
	private String identityPassword;
	@Parameter(names = "-blob_format", description = "Blob format (base64, hex)", converter = BlobConverter.class, validateWith = BlobConverter.class)
	private Blob blob;
	@Parameter(names = "-boolean_format_true", description = "Format e.g. \"true\"")
	private String booleanTruePattern;
	@Parameter(names = "-boolean_format_false", description = "Format e.g. \"false\"")
	private String booleanFalsePattern;
	@Parameter(names = "-datetime_format", description = "Date/time format")
	private String datePattern;
	@Parameter(names = "-number_format", description = "Number format")
	private String numberPattern;
	@Parameter(names = "-time_zone", description = "Time zone ID e.g. GMT")
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
			LogFactory.getRootLogger(logLevel, logFile, logFilePattern,
					logFileLimit, logFileCount, logFileAppend);
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
		IChannel channel;
		try {
			ChannelConfig config = getChannelConfig();
			config.setMetaspace(metaspaceName);
			Member member = new Member();
			member.setDataStore(dataStore);
			member.setDiscovery(discovery);
			member.setIdentityPassword(identityPassword);
			member.setListen(listen);
			member.setMemberName(memberName);
			member.setRxBufferSize(rxBufferSize);
			member.setSecurityTokenFile(securityToken);
			member.setWorkerThreadCount(workerThreadCount);
			config.setMember(member);
			ConversionConfig conversionConfig = config.getConversion();
			conversionConfig.setBlob(blob);
			conversionConfig.setBooleanTruePattern(booleanTruePattern);
			conversionConfig.setBooleanFalsePattern(booleanFalsePattern);
			conversionConfig.setDatePattern(datePattern);
			conversionConfig.setNumberPattern(numberPattern);
			conversionConfig.setTimeZoneID(timeZoneID);
			for (ICommand command : commands) {
				command.configure(config.getDestinations());
			}
			channel = getChannel(config);
			channel.addListener(new DestinationMonitor());
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not create channel", e);
			return;
		}
		execute(channel);
	}

	protected void execute(IChannel channel) {
		try {
			channel.start();
			channel.awaitTermination();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not start channel", e);
		} finally {
			try {
				channel.stop();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not stop channel", e);
			}
		}
	}

	protected ChannelConfig getChannelConfig() throws Exception {
		return new ChannelConfig();
	}

	protected abstract IChannel getChannel(ChannelConfig config);

	protected ICommand getDefaultCommand() {
		return null;
	}

	protected void addCommands(JCommander jc) {
	}

	protected abstract String getProgramName();

}
