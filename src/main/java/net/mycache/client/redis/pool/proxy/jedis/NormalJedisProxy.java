package net.mycache.client.redis.pool.proxy.jedis;

import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import net.mycache.client.redis.OPeration;
import net.mycache.client.redis.model.proxy.JedisProxy;
import net.mycache.client.redis.pool.JedisRandomConnectionPool;
import net.mycache.client.redis.pool.proxy.IBasePoolProxy;
import com.google.common.collect.Sets;

/**
 * 分布式集群RedisPoolProxy
 * 
 */
public class NormalJedisProxy implements IBasePoolProxy<JedisProxy> {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private JedisRandomConnectionPool jedisPool;

	public NormalJedisProxy(GenericObjectPoolConfig poolConfig, Integer timeOut, String hostIp) {
		Set<HostAndPort> jedisClusterNodes = getJedisInfoList(hostIp, timeOut);
		jedisPool = new JedisRandomConnectionPool(jedisClusterNodes, poolConfig, timeOut);
	}

	private Set<HostAndPort> getJedisInfoList(String hostIp, Integer timeOut) {
		Set<HostAndPort> jedisClusterNodes = Sets.newHashSet();
		String[] ips = hostIp.split(";");
		for (String ip : ips) {
			String[] host = ip.split(",");
			if (host.length < 2)
				continue;
			jedisClusterNodes.add(new HostAndPort(host[0], Integer.parseInt(host[1])));
		}
		return jedisClusterNodes;
	}

	public JedisProxy getResource() {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getConnection();
		} catch (Exception e) {
			logger.error("Err : get resource failed.", e);
			jedisPool.returnBrokenConnection(jedis);
		}
		return new JedisProxy(jedis);
	}

	public void returnBrokenResource(JedisProxy jedis) {
		jedisPool.returnBrokenConnection(jedis.getJedis());
	}

	public void returnResource(JedisProxy jedis) {
		jedisPool.returnConnection(jedis.getJedis());
	}

	public void destroy() {
		// @Todo destroy
	}

	public void destroySlave() {
		this.destroy();
	}

	public JedisProxy getSlaveResource() {
		return this.getResource();
	}

	public void returnBrokenSlaveResource(JedisProxy jedis) {
		this.returnBrokenResource(jedis);
	}

	public void returnSlaveResource(JedisProxy jedis) {
		this.returnResource(jedis);
	}

	public JedisProxy getResource(OPeration operation) {
		return getResource();
	}
}
