package net.mycache.core;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.util.ShardInfo;
import redis.clients.util.Sharded;

public class JRedisShardInfo extends ShardInfo<JedisCommands> {
	private JedisShardInfo info;
	private Integer failedTimes = 0;
	private Boolean available;
	private final static int MAX_FAILED_TIMES = 10;
	private GenericObjectPoolConfig poolConfig;
	private List<JedisShardInfo> infos;
	private JRedisPool jredisPool;
	
	public JRedisShardInfo(JedisShardInfo info) {
		super(Sharded.DEFAULT_WEIGHT);

		this.info = info;
		this.available = true;
	}
	
	public JRedisShardInfo(final GenericObjectPoolConfig poolConfig, List<JedisShardInfo> infos) {
		super(Sharded.DEFAULT_WEIGHT);

		this.poolConfig = poolConfig;
		this.infos = infos;
	}
	
	private void incFailedTimes() {
		failedTimes++;
	}
	
	private Boolean isNeededDetect() {
		return failedTimes > MAX_FAILED_TIMES;
	}
	
	public synchronized void handleError() {
		incFailedTimes();
		if (isNeededDetect()) {
			
		}
	}
	
	public Boolean isAvailable() {
		return this.available;
	}
    
	@Override
    public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{jedisShardInfos:");
		builder.append(infos);
		builder.append(",failedTimes:");
		builder.append(failedTimes);
		builder.append(",available:");
		builder.append(available);
		builder.append(",genericObjectPoolConfig:");
		builder.append("{");
		builder.append("maxIdle:");
		builder.append(poolConfig.getMaxIdle());
		builder.append(",maxTotal");
		builder.append(poolConfig.getMaxTotal());
		builder.append(",maxWaitMillis:");
		builder.append(poolConfig.getMaxWaitMillis());
		builder.append(",minEvictableIdleTimeMillis:");
		builder.append(poolConfig.getMinEvictableIdleTimeMillis());
		builder.append(",minIdle:");
		builder.append(poolConfig.getMinIdle());
		builder.append("}");
		builder.append(",jredisPool:");
		builder.append(jredisPool);
		builder.append("}");
    	return builder.toString();
    }
    
	@Override
	protected JedisCommands createResource() {
		String host = "";
		String password = "";
		int timeout = 2000;
		
		for (JedisShardInfo info : infos) {
			host += info.getHost() + ":" + info.getPort() + ";";
			password = info.getPassword();
			timeout = info.getSoTimeout();
		}
		/*去除最后一个分号*/
		host = host.substring(0, host.length() - 1);
		jredisPool = new JRedisPool(poolConfig, host, timeout, password);
		return jredisPool.getJRedis();
	}

	@Override
	public String getName() {
		return null;
	}
}
