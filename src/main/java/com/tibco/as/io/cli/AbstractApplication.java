package com.tibco.as.io.cli;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.cli.converters.LogLevelConverter;
import com.tibco.as.log.LogFactory;
import com.tibco.as.log.LogLevel;

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
	@Parameter(names = { "-metaspace" }, description = "Metaspace name")
	private String metaspaceName;
	@Parameter(names = { "-member_name" }, description = "Member name")
	private String memberName;
	@Parameter(names = { "-discovery" }, description = "Discovery URL")
	private String discovery;
	@Parameter(names = { "-listen" }, description = "Listen URL")
	private String listen;
	@Parameter(names = { "-rx_buffer_size" }, description = "Receive buffer size")
	private Long rxBufferSize;
	@Parameter(names = { "-worker_thread_count" }, description = "Worker thread count")
	private Integer workerThreadCount;
	@Parameter(names = { "-data_store" }, description = "Directory path for data store")
	private String dataStore;
	@Parameter(names = { "-no_exit" }, description = "Do not shut down after application execution")
	private boolean noExit;
	@Parameter(names = { "-security_token" }, description = "Security token path")
	private String securityToken;
	@Parameter(names = { "-identity_password" }, description = "Identity password")
	private String identityPassword;
	@Parameter(names = { "-parallel" }, description = "Enable parallel destinations")
	private boolean parallel;

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
			config.setDataStore(dataStore);
			config.setDiscovery(discovery);
			config.setIdentityPassword(identityPassword);
			config.setListen(listen);
			config.setMember(memberName);
			config.setMetaspace(metaspaceName);
			config.setRxBufferSize(rxBufferSize);
			config.setSecurityTokenFile(securityToken);
			config.setWorkerThreadCount(workerThreadCount);
			config.setSequential(parallel);
			for (ICommand command : commands) {
				command.configure(config);
			}
			channel = getChannel(config);
			if (config.isSequential()) {
				channel.addListener(new DestinationMonitor());
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not create channel", e);
			return;
		}
		execute(channel);
		if (noExit) {
			while (true) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					log.log(Level.SEVERE, "Interrupted", e);
				}
			}
		}
	}

	protected void execute(IChannel channel) {
		try {
			channel.start();
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

	protected abstract ChannelConfig getChannelConfig() throws Exception;

	protected abstract IChannel getChannel(ChannelConfig config);

	protected ICommand getDefaultCommand() {
		return null;
	}

	protected void addCommands(JCommander jc) {
	}

	protected abstract String getProgramName();

}
