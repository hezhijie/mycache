package net.mycache.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class ShardedJRedis {
	private Sharded<JedisCommands, JRedisShardInfo>  shard;
	private final int NODE_SIZES_PER_SHARD = 2;
	private List<JRedisShardInfo> shardinfos = new ArrayList<JRedisShardInfo>();
	
	public ShardedJRedis(final GenericObjectPoolConfig poolConfig,
			List<JedisShardInfo> infos, Hashing algo, Pattern keyTagPattern) {
		if (infos.size() % NODE_SIZES_PER_SHARD != 0) {
			throw new RuntimeException("node size for shard must be the time of " + NODE_SIZES_PER_SHARD);
		}
		
		int start = 0;
		for (int i = 0; i < infos.size(); i++) {
			if (i % NODE_SIZES_PER_SHARD == NODE_SIZES_PER_SHARD - 1) {
				shardinfos.add(new JRedisShardInfo(poolConfig, infos.subList(start, i+1)));
				start += NODE_SIZES_PER_SHARD;
			}
		}

		this.shard = new Sharded<JedisCommands, JRedisShardInfo>(shardinfos, algo, keyTagPattern);
	}

	public Object invoke(Method method, Object[] args) {
		Object result = null;
		JedisCommands commands = null;
		if(method.getName().equals("blpop")){
		  commands = shard.getShard((String)args[1]);
		}else{
		  commands = shard.getShard((String)args[0]);
		}
		try {
			result = method.invoke(commands, args);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			result = null;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			result = null;
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			result = null;
		}
		
		return result;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{jredisShardInfo:");
		builder.append(shardinfos);
		builder.append("}");
		return builder.toString();
	}
}
