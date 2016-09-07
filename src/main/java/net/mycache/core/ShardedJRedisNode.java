package net.mycache.core;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class ShardedJRedisNode {
	private ShardedJedisPool pool;
	private ShardedJedis shard;
	private Map<String, JRedisShardInfo> infos = new HashMap<String, JRedisShardInfo>();
	
	public ShardedJRedisNode(List<JedisShardInfo> infos, ShardedJedisPool pool) {
		this.pool = pool;
		this.shard = this.pool.getResource();
		for (JedisShardInfo info : infos) {
			JRedisShardInfo shardInfo = new JRedisShardInfo(info);
			this.infos.put(info.toString(), shardInfo);
		}
	}
	
	public JRedisShardInfo getJRedis(String key) {
		JedisShardInfo info = shard.getShardInfo(key);
		return infos.get(info.toString());
	}
	
	public Boolean isAvaliable(String key) {
		return getJRedis(key).isAvailable();
	}
	
	public ShardedJedisPool getPool() {
		return this.pool;
	}
	
	public Object invoke(Method method, Object[] args) throws Exception {
		Object result = null;
		String key = (String)args[0];
		
		Boolean broken = false;
		ShardedJedis shardedJedis = null;
		JRedisShardInfo shardedInfo = getJRedis(key);
		if (shardedInfo.isAvailable()) {
			shardedJedis = pool.getResource();
			try {
				result = method.invoke(shardedJedis, args);
			} catch (Exception e) {
				e.printStackTrace();
				shardedInfo.handleError();
				throw e;
			} finally {
				if (shardedJedis != null) {
					if (broken) {
						pool.returnBrokenResource(shardedJedis);
					} else {
						pool.returnResource(shardedJedis);
					}
				}
			}
		}
		return result;
	}
	
	public void destory() {
		if (shard != null) {
			pool.returnResource(shard);
		}
		
		if (pool != null) {
			pool.destroy();
		}
	}
}
