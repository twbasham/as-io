package com.tibco.as.io.cli;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.tibco.as.io.EventManager;
import com.tibco.as.io.IEvent;
import com.tibco.as.io.IEventListener;
import com.tibco.as.space.ASException;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Utils;

public abstract class AbstractApplication implements IEventListener {

	@Parameter(names = { "-?", "-help" }, description = "Print this help message", help = true)
	private Boolean help;
	@Parameter(names = "-log_level", converter = LogLevelConverter.class, validateWith = LogLevelConverter.class, description = "Log level (ERROR, WARNING, INFO, DEBUG or VERBOSE)")
	private LogLevel logLevel = LogLevel.INFO;
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

	private Logger log;

	protected AbstractApplication() {
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream in = classLoader.getResourceAsStream("logging.properties");
		try {
			LogManager.getLogManager().readConfiguration(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		EventManager.addListener(this);
		log = Logger.getLogger(AbstractApplication.class.getName());
	}

	public String getMetaspaceName() {
		return metaspaceName;
	}

	@Override
	public void onEvent(IEvent event) {
		switch (event.getSeverity()) {
		case DEBUG:
			log.fine(event.getMessage());
			break;
		case INFO:
			log.info(event.getMessage());
			break;
		case WARN:
			log.warning(event.getMessage());
			break;
		case ERROR:
			log.log(Level.SEVERE, event.getMessage(), event.getException());
			break;
		}
	}

	public void execute(String[] args) {
		JCommander jc = new JCommander(this);
		jc.setProgramName(getProgramName());
		addCommands(jc);
		try {
			jc.parse(args);
		} catch (ParameterException e) {
			System.err.println(e.getLocalizedMessage());
			return;
		}
		if (args.length == 0 || Boolean.TRUE.equals(help)) {
			jc.usage();
			return;
		}
		setLogLevel();
		List<AbstractCommand> commands = new ArrayList<AbstractCommand>();
		String parsedCommand = jc.getParsedCommand();
		if (parsedCommand == null) {
			AbstractCommand defaultCommand = getDefaultCommand();
			if (defaultCommand == null) {
				log.warning("No command specified");
				jc.usage();
				return;
			}
			commands.add(defaultCommand);
		} else {
			JCommander commandJC = jc.getCommands().get(parsedCommand);
			for (Object command : commandJC.getObjects()) {
				commands.add((AbstractCommand) command);
			}
		}
		for (AbstractCommand command : commands) {
			try {
				command.prepare();
			} catch (Exception e) {
				System.err.println(e.getLocalizedMessage());
				return;
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
		for (AbstractCommand command : commands) {
			command.execute(metaspace);
		}
		if (Boolean.TRUE.equals(noExit)) {
			while (true) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void setLogLevel() {
		if (logLevel == null) {
			return;
		}
		Logger.getLogger("").setLevel(logLevel.getLevel());
	}

	protected AbstractCommand getDefaultCommand() {
		return null;
	}

	protected void addCommands(JCommander jc) {
	}

	protected abstract String getProgramName();

}
