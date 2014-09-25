package com.tibco.as.io.cli;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.tibco.as.io.IChannel;
import com.tibco.as.log.LogFactory;
import com.tibco.as.log.LogLevel;
import com.tibco.as.space.ASException;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Utils;

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
	private Boolean noExit;
	@Parameter(names = { "-security_token" }, description = "Security token path")
	private String securityToken;
	@Parameter(names = { "-identity_password" }, description = "Identity password")
	private String identityPassword;

	protected AbstractApplication() {
	}

	public String getMetaspaceName() {
		return metaspaceName;
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
		Metaspace metaspace = Utils.getMetaspace(metaspaceName);
		if (metaspace == null) {
			MemberDef memberDef = MemberDef.create();
			if (memberName != null) {
				memberDef.setMemberName(memberName);
			}
			if (discovery != null) {
				memberDef.setDiscovery(discovery);
			}
			if (listen != null) {
				memberDef.setListen(listen);
			}
			if (dataStore != null) {
				memberDef.setDataStore(dataStore);
			}
			if (rxBufferSize != null) {
				memberDef.setRxBufferSize(rxBufferSize);
			}
			if (workerThreadCount != null) {
				memberDef.setWorkerThreadCount(workerThreadCount);
			}
			if (securityToken != null) {
				memberDef.setSecurityTokenFile(securityToken);
			}
			if (identityPassword != null) {
				memberDef.setIdentityPassword(identityPassword.toCharArray());
			}
			try {
				metaspace = Metaspace.connect(metaspaceName, memberDef);
			} catch (ASException e) {
				System.err.println(e.getLocalizedMessage());
				return;
			}
		}
		IChannel channel;
		try {
			channel = getChannel(metaspace);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not create channel", e);
			return;
		}
		for (ICommand command : commands) {
			try {
				command.configure(channel);
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not run command", e);
			}
		}
		try {
			channel.open();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not open channel", e);
		} finally {
			try {
				channel.close();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not close channel", e);
			}
		}
		if (Boolean.TRUE.equals(noExit)) {
			while (true) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					log.log(Level.SEVERE, "Interrupted", e);
				}
			}
		}
	}

	protected abstract IChannel getChannel(Metaspace metaspace)
			throws Exception;

	protected ICommand getDefaultCommand() {
		return null;
	}

	protected void addCommands(JCommander jc) {
	}

	protected abstract String getProgramName();

}
