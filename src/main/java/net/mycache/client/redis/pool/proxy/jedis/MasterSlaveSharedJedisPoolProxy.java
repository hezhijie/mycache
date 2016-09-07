package net.mycache.client.redis.pool.proxy.jedis;

import java.util.List;
import java.util.Set;

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
import com.google.common.collect.Sets;

/**
 * M/S集群ShardedRedisPoolProxy
 * 
 */
public class MasterSlaveSharedJedisPoolProxy implements IBasePoolProxy<ShardedJedisProxy> {

	private final Logger logger = LoggerFactory.getLogger(MasterSlaveSharedJedisPoolProxy.class);
	
	private String hosts; //TODO  不应该从这里传进来， 应该是从最外层的配置对象取， 不然参数藏的比较深
	
	
	
	/**
	 * JedisPool-Master
	 */
	private static ShardedJedisPool jedisMasterPool;

	/**
	 * JedisPool-Slave
	 */
	private static ShardedJedisPool jedisSlavePool;

	/**
	 * Alis-List
	 */
	private Set<String> nameSet;

	/**
	 * Constractor
	 * 
	 * @param poolConfig
	 *            config
	 * @param timeOut
	 *            超时
	 * @param masterHostIp
	 *            MasterIp
	 * @param slaveHostIp
	 *            SlaveIp
	 */
	public MasterSlaveSharedJedisPoolProxy(GenericObjectPoolConfig poolConfig, Integer timeOut, String hostIp) {

		nameSet = Sets.newHashSet();
		String hostIps[] = hostIp.split(hosts);

		// 初始化主实例组
		List<JedisShardInfo> masterLst = getMasterJedisInfoList(hostIps[0], timeOut);
		jedisMasterPool = new ShardedJedisPool(poolConfig, masterLst);

		// 初始化从实例组
		List<JedisShardInfo> slaveLst = getSlaveJedisInfoList(hostIps[1], timeOut);
		jedisSlavePool = new ShardedJedisPool(poolConfig, slaveLst);
	}

	private List<JedisShardInfo> getMasterJedisInfoList(String hostIp, Integer timeOut) {
		List<JedisShardInfo> lst = Lists.newArrayList();
		String[] ips = hostIp.split(";");
		for (String ip : ips) {
			String[] host = ip.split(",");
			if (host.length < 3)
				continue;
			if (nameSet.add(host[2])) {
				lst.add(new JedisShardInfo(host[0], Integer.parseInt(host[1]), timeOut, host[2]));
			} else {
				throw new RuntimeException("The host name is repeat");
			}
		}
		return lst;
	}

	private List<JedisShardInfo> getSlaveJedisInfoList(String hostIp, Integer timeOut) {
		List<JedisShardInfo> lst = Lists.newArrayList();
		String[] ips = hostIp.split(";");
		for (String ip : ips) {
			String[] host = ip.split(",");
			if (host.length < 3)
				continue;
			if (nameSet.remove(host[2])) {
				lst.add(new JedisShardInfo(host[0], Integer.parseInt(host[1]), timeOut, host[2]));
			} else {
				throw new RuntimeException("Unknown slave name that can't match to a exits master");
			}
			if (!nameSet.isEmpty())
				throw new RuntimeException("Count of slave can't match the master");
		}
		return lst;
	}

	public void destroy() {
		jedisMasterPool.destroy();
	}

	public void destroySlave() {
		jedisSlavePool.destroy();
	}

	public ShardedJedisProxy getResource() {
		ShardedJedis jedis = null;
		try {
			jedis = jedisMasterPool.getResource();
		} catch (Exception e) {
			logger.error("Err : get resource failed.", e);
			jedisMasterPool.returnBrokenResource(jedis);
		}
		return new ShardedJedisProxy(jedis);
	}

	public ShardedJedisProxy getSlaveResource() {
		ShardedJedis jedis = null;
		try {
			jedis = jedisSlavePool.getResource();
		} catch (Exception e) {
			jedisSlavePool.returnBrokenResource(jedis);
			logger.error("Err : get resource failed.", e);
		}
		return new ShardedJedisProxy(jedis);
	}

	public void returnBrokenResource(ShardedJedisProxy jedis) {
		jedisMasterPool.returnBrokenResource(jedis.getJedis());
	}

	public void returnBrokenSlaveResource(ShardedJedisProxy jedis) {
		jedisSlavePool.returnBrokenResource(jedis.getJedis());
	}

	public void returnResource(ShardedJedisProxy jedis) {
		jedisMasterPool.returnResource(jedis.getJedis());
	}

	public void returnSlaveResource(ShardedJedisProxy jedis) {
		jedisSlavePool.returnResource(jedis.getJedis());
	}

	public ShardedJedisProxy getResource(OPeration operation) {
		switch (operation) {
		case READ:
			return getSlaveResource();
		case READWRITE:
			return getResource();
		case WRITE:
			return getResource();
		default:
			return getResource();
		}
	}

}
