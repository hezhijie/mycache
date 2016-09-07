package net.mycache.core;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class HAConfig {
	private GenericObjectPoolConfig poolConfig;
	private List<ServerInfo> servers;

	public HAConfig(final GenericObjectPoolConfig poolConfig, List<ServerInfo> servers) {
		this.poolConfig = poolConfig;
		this.servers = servers;
	}
	
	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}
	
	public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}
	
	public List<ServerInfo> getServers() {
		return servers;
	}
	
	public void setServers(List<ServerInfo> servers) {
		this.servers = servers;
	}
}
