package net.mycache.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;

public class ShardedJRedisPool {

	private JedisCommands jedis; 
	private ShardedJRedisSet nodeSet;
	private ShardedJRedis shardedJRedis;
	
	public ShardedJRedisPool(final GenericObjectPoolConfig poolConfig,
            List<JedisShardInfo> shardInfos) {
		this(poolConfig, shardInfos, Hashing.MURMUR_HASH);
	}
	
	public ShardedJRedisPool(final GenericObjectPoolConfig poolConfig,
            List<JedisShardInfo> shardInfos, Hashing algo) {
		this(poolConfig, shardInfos, Hashing.MURMUR_HASH, null);
	}
	
	public ShardedJRedisPool(final GenericObjectPoolConfig poolConfig,
			List<JedisShardInfo> shardInfos, List<JedisShardInfo> infos1,
			Pattern keyTagPattern) {
		this(poolConfig, shardInfos, Hashing.MURMUR_HASH, keyTagPattern);
	}
	
	public ShardedJRedisPool(final GenericObjectPoolConfig poolConfig,
			List<JedisShardInfo> shardInfos, Hashing algo, Pattern keyTagPattern) {
		shardedJRedis = new ShardedJRedis(poolConfig, shardInfos, algo, keyTagPattern);
		
		//nodeSet = new ShardedJRedisSet(poolConfig, shardInfos, algo, keyTagPattern);

        jedis = (JedisCommands) Proxy.newProxyInstance(JedisCommands.class.getClassLoader(), 
        		new Class[] { JedisCommands.class }, new RedisGroupInvocationHandler());
	
	}
	
	public JedisCommands getResource() {
		return jedis;
	}
	
	private Object invokeRead(Method method, Object[] args) {
		Object result = null;
		String key = (String)args[0];
		
		for (ShardedJRedisNode node : nodeSet.getShardedJRedisNodes()) {
			if (node.isAvaliable(key)) {
				ShardedJedis shardedJedis = node.getPool().getResource();
				try {
					result = method.invoke(shardedJedis, args);
					return result;
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					
				}
			}
		}
		
		return result;
	}
	
	private Object invokeWrite(Method method, Object[] args) {
		Object result = null;
		String key = (String)args[0];
		
		for (ShardedJRedisNode node : nodeSet.getShardedJRedisNodes()) {
			try {
				node.invoke(method, args);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		
		return result;
	}
	
    public class RedisGroupInvocationHandler implements InvocationHandler {

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			
			return shardedJRedis.invoke(method, args);
			
//			if (JRedisCommand.isReadCommand(method.getName())) {
//				return invokeRead(method, args);
//			} else {
//				return invokeWrite(method, args);
//			}
		}
    }

    public void returnBrokenResource(JedisCommands jedis) {
      // TODO return the broken resource
    }

    public void returnResource(JedisCommands jedis) {
      // TODO return the resource
    }

    public void destroy() {
      // TODO destroy the pool
    }
    
    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	builder.append("{shardedJRedis:");
    	builder.append(shardedJRedis);
    	builder.append("}");
    	return builder.toString();
    }
}
