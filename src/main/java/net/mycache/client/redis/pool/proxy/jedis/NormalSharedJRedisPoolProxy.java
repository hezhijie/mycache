package net.mycache.client.redis.pool.proxy.jedis;

import java.util.List;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisShardInfo;
import net.mycache.client.redis.OPeration;
import net.mycache.client.redis.model.proxy.JedisCommondsProxy;
import net.mycache.client.redis.pool.proxy.IBasePoolProxy;
import net.mycache.core.ShardedJRedisPool;

import com.google.common.collect.Lists;

/**
 * 一致性哈希ShardedRedisPoolProxy
 */
public class NormalSharedJRedisPoolProxy implements IBasePoolProxy<JedisCommondsProxy> {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private ShardedJRedisPool jedisPool;
	
	private GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
	private Integer timeOut = 1000;
	private String hostIp = "127.0.0.1:6379;127.0.0.1:6479;127.0.0.1:6579;127.0.0.1:6679;";
	
	public NormalSharedJRedisPoolProxy(){
		initPool(poolConfig,timeOut,hostIp);
	}
	
	public NormalSharedJRedisPoolProxy(GenericObjectPoolConfig poolConfig, Integer timeOut, String hostIp) {
		initPool(poolConfig,timeOut,hostIp);
	}

	public void initPool(GenericObjectPoolConfig poolConfig, Integer timeOut, String hostIp){
		List<JedisShardInfo> lst = getJedisInfoList(hostIp, timeOut);
		jedisPool = new ShardedJRedisPool(poolConfig, lst);
	}
	
	private List<JedisShardInfo> getJedisInfoList(String hostIp, Integer timeOut) {
		List<JedisShardInfo> lst = Lists.newArrayList();
		String[] ips = hostIp.split(";");
		for (String ip : ips) {
			String[] host = ip.split(":");
			if (host.length < 2)
				continue;
			lst.add(new JedisShardInfo(host[0], Integer.parseInt(host[1]), timeOut));
		}
		return lst;
	}

	public JedisCommondsProxy getResource() {
		JedisCommands jedis = null;
		try {
			jedis = jedisPool.getResource();
		} catch (Exception e) {
			logger.error("Err : get resource failed.", e);
			jedisPool.returnBrokenResource(jedis);
		}
		return new JedisCommondsProxy(jedis);
	}

	public void returnBrokenResource(JedisCommondsProxy jedis) {
		jedisPool.returnBrokenResource(jedis.getJedis());
	}

	public void returnResource(JedisCommondsProxy jedis) {
		jedisPool.returnResource(jedis.getJedis());
	}

	public void destroy() {
		jedisPool.destroy();
	}

	public void destroySlave() {
		this.destroy();
	}

	public JedisCommondsProxy getSlaveResource() {
		return this.getResource();
	}

	public void returnBrokenSlaveResource(JedisCommondsProxy jedis) {
		this.returnBrokenResource(jedis);
	}

	public void returnSlaveResource(JedisCommondsProxy jedis) {
		this.returnResource(jedis);
	}

	public JedisCommondsProxy getResource(OPeration operation) {
		return getResource();
	}

	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	public Integer getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(Integer timeOut) {
		this.timeOut = timeOut;
	}

	public String getHostIp() {
		return hostIp;
	}

	public void setHostIp(String hostIp) {
		this.hostIp = hostIp;
	}
	

}
