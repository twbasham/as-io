package com.tibco.as.io;

import java.util.ArrayList;
import java.util.List;

public class ChannelConfig {

	private String metaspace;
	private String member;
	private String discovery;
	private String listen;
	private String dataStore;
	private Long rxBufferSize;
	private Integer workerThreadCount;
	private String securityTokenFile;
	private String securityPolicyFile;
	private String identityPassword;
	private Integer clusterSuspendThreshold;
	private Long connectTimeout;
	private Long memberTimeout;
	private String remoteDiscovery;
	private String remoteListen;
	private Integer transportThreadCount;
	private List<DestinationConfig> destinations = new ArrayList<DestinationConfig>();

	public String getMetaspace() {
		return metaspace;
	}

	public void setMetaspace(String metaspace) {
		this.metaspace = metaspace;
	}

	public String getMember() {
		return member;
	}

	public void setMember(String member) {
		this.member = member;
	}

	public String getDiscovery() {
		return discovery;
	}

	public void setDiscovery(String discovery) {
		this.discovery = discovery;
	}

	public String getListen() {
		return listen;
	}

	public void setListen(String listen) {
		this.listen = listen;
	}

	public String getDataStore() {
		return dataStore;
	}

	public void setDataStore(String dataStore) {
		this.dataStore = dataStore;
	}

	public Long getRxBufferSize() {
		return rxBufferSize;
	}

	public void setRxBufferSize(Long rxBufferSize) {
		this.rxBufferSize = rxBufferSize;
	}

	public Integer getWorkerThreadCount() {
		return workerThreadCount;
	}

	public void setWorkerThreadCount(Integer workerThreadCount) {
		this.workerThreadCount = workerThreadCount;
	}

	public String getSecurityTokenFile() {
		return securityTokenFile;
	}

	public void setSecurityTokenFile(String securityTokenFile) {
		this.securityTokenFile = securityTokenFile;
	}

	public String getIdentityPassword() {
		return identityPassword;
	}

	public void setIdentityPassword(String identityPassword) {
		this.identityPassword = identityPassword;
	}

	public List<DestinationConfig> getDestinations() {
		return destinations;
	}

	public Integer getClusterSuspendThreshold() {
		return clusterSuspendThreshold;
	}

	public void setClusterSuspendThreshold(Integer clusterSuspendThreshold) {
		this.clusterSuspendThreshold = clusterSuspendThreshold;
	}

	public Long getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(Long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public Long getMemberTimeout() {
		return memberTimeout;
	}

	public void setMemberTimeout(Long memberTimeout) {
		this.memberTimeout = memberTimeout;
	}

	public String getRemoteDiscovery() {
		return remoteDiscovery;
	}

	public void setRemoteDiscovery(String remoteDiscovery) {
		this.remoteDiscovery = remoteDiscovery;
	}

	public String getRemoteListen() {
		return remoteListen;
	}

	public void setRemoteListen(String remoteListen) {
		this.remoteListen = remoteListen;
	}

	public String getSecurityPolicyFile() {
		return securityPolicyFile;
	}

	public void setSecurityPolicyFile(String securityPolicyFile) {
		this.securityPolicyFile = securityPolicyFile;
	}

	public Integer getTransportThreadCount() {
		return transportThreadCount;
	}

	public void setTransportThreadCount(Integer transportThreadCount) {
		this.transportThreadCount = transportThreadCount;
	}

}
