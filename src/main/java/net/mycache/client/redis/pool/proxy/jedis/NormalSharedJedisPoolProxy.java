package net.mycache.client.redis.pool.proxy.jedis;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import net.mycache.client.redis.OPeration;
import net.mycache.client.redis.model.proxy.ShardedJedisProxy;
import net.mycache.client.redis.pool.proxy.IBasePoolProxy;
import com.google.common.collect.Lists;

/**
 * 一致性哈希ShardedRedisPoolProxy
 * 
 */
public class NormalSharedJedisPoolProxy implements IBasePoolProxy<ShardedJedisProxy> {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private static ShardedJedisPool jedisPool;

	public NormalSharedJedisPoolProxy(GenericObjectPoolConfig poolConfig, Integer timeOut, String hostIp) {
		List<JedisShardInfo> lst = getJedisInfoList(hostIp, timeOut);
		jedisPool = new ShardedJedisPool(poolConfig, lst);
	}

	private List<JedisShardInfo> getJedisInfoList(String hostIp, Integer timeOut) {
		List<JedisShardInfo> lst = Lists.newArrayList();
		String[] ips = hostIp.split(";");
		for (String ip : ips) {
			String[] host = ip.split(",");
			if (host.length < 3)
				continue;
			lst.add(new JedisShardInfo(host[0], Integer.parseInt(host[1]), timeOut, host[2]));
		}
		return lst;
	}

	public ShardedJedisProxy getResource() {
		ShardedJedis jedis = null;
		try {
			jedis = jedisPool.getResource();
		} catch (Exception e) {
			logger.error("Err : get resource failed.", e);
			jedisPool.returnBrokenResource(jedis);
		}
		return new ShardedJedisProxy(jedis);
	}

	public void returnBrokenResource(ShardedJedisProxy jedis) {
		jedisPool.returnBrokenResource(jedis.getJedis());
	}

	public void returnResource(ShardedJedisProxy jedis) {
		jedisPool.returnResource(jedis.getJedis());
	}

	public void destroy() {
		jedisPool.destroy();
	}

	public void destroySlave() {
		this.destroy();
	}

	public ShardedJedisProxy getSlaveResource() {
		return this.getResource();
	}

	public void returnBrokenSlaveResource(ShardedJedisProxy jedis) {
		this.returnBrokenResource(jedis);
	}

	public void returnSlaveResource(ShardedJedisProxy jedis) {
		this.returnResource(jedis);
	}

	public ShardedJedisProxy getResource(OPeration operation) {
		return getResource();
	}

}
