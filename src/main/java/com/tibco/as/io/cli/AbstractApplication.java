package com.tibco.as.io.cli;

import java.util.ArrayList;
import java.util.List;

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
	@Parameter(names = { "-no_exit" }, description = "Do not shut down after the Eclipse application has ended")
	private Boolean noExit;
	@Parameter(names = { "-security_token" }, description = "Security token path")
	private String securityToken;

	protected AbstractApplication() {
		EventManager.addListener(this);
	}
	
	public String getMetaspaceName() {
		return metaspaceName;
	}

	@Override
	public void onEvent(IEvent event) {
		switch (event.getSeverity()) {
		case DEBUG:
		case INFO:
			System.out.println(event.getMessage());
			break;
		case WARN:
		case ERROR:
			System.err.println(event.getMessage());
			if (event.getException() != null) {
				event.getException().printStackTrace();
			}
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
		List<AbstractCommand> commands = new ArrayList<AbstractCommand>();
		String parsedCommand = jc.getParsedCommand();
		if (parsedCommand == null) {
			AbstractCommand defaultCommand = getDefaultCommand();
			if (defaultCommand == null) {
				System.out.println("No command specified");
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

	protected AbstractCommand getDefaultCommand() {
		return null;
	}

	protected void addCommands(JCommander jc) {
	}

	protected abstract String getProgramName();

}
