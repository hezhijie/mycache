package net.mycache.core;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ServerInfo {
	private String host;
	private int port;
	private int timeout;
	private String password;
	private GenericObjectPoolConfig poolConfig;

	public ServerInfo(GenericObjectPoolConfig poolConfig, String host, int port, 
			int timeout, String password) {
		this.poolConfig = poolConfig;
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.password = password;
	}
	
    public String generateKey() {
    	return host + "_" + port + "_" + timeout + "_" + password;
    }
	
	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getTimeout() {
		return timeout;
	}
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}	
	
	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}
	
    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	builder.append("{host:");
    	builder.append(host);
    	builder.append(",port:");
    	builder.append(port);
    	builder.append(",timeout:");
    	builder.append(timeout);
    	builder.append(",password:");
    	builder.append(password);
    	builder.append("}");
    	return builder.toString();    	
    }
}
