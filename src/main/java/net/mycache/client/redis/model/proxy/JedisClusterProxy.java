package net.mycache.client.redis.model.proxy;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import net.mycache.client.redis.model.proxy.pipeline.IPipeLineProxy;

public class JedisClusterProxy implements IJedisProxy<JedisCluster> {

  public JedisClusterProxy(JedisCluster jedis) {
    this(jedis, false);
  }

  public JedisClusterProxy(JedisCluster jedis, boolean broken) {
    this.jedis = jedis;
    this.broken = broken;
  }

  private JedisCluster jedis;

  private boolean broken;

  public boolean isBroken() {
    return this.broken;
  }

  public void setBroken(boolean broken) {
    this.broken = broken;
  }

  public JedisCluster getJedis() {
    return this.jedis;
  }

  public void setJedis(JedisCluster jedis) {
    this.jedis = jedis;
  }

  public boolean exists(String key) {
    return this.jedis.exists(key);
  }

  public Long del(String key) {
    return this.jedis.del(key);
  }

  public void rpush(String key, String aList) {
    this.jedis.rpush(key, aList);
  }

  public String lpop(String key) {
    return this.jedis.lpop(key);
  }
  
  public void lpush(String key, String aList) {
    this.jedis.lpush(key, aList);
  }

  public String rpop(String key) {
    return this.jedis.rpop(key);
  }
  
  public Long llen(String key) {
    return this.jedis.llen(key);
  }
  
  
  public void hset(String key, String field, String value) {
    this.jedis.hset(key, field, value);
  }

  public void hincrBy(String key, String incrementField, long incrementValue) {
    this.jedis.hincrBy(key, incrementField, incrementValue);

  }

  public String get(String key) {
    return this.jedis.get(key);
  }

  public List<String> lrange(String key, int i, int j) {
    return this.jedis.lrange(key, i, j);
  }

  public Map<String, String> hgetAll(String key) {
    return this.jedis.hgetAll(key);
  }

  public String hget(String key, String field) {
    return this.jedis.hget(key, field);
  }

  public void hmset(String key, Map<String, String> map) {
    this.jedis.hmset(key, map);
  }

  public List<String> hmget(String key, String... field) {
    return this.jedis.hmget(key, field);
  }

  public String getSet(String key, String value) {
    return this.jedis.getSet(key, value);
  }

  public void expire(String key, int cacheSeconds) {
    this.jedis.expire(key, cacheSeconds);
  }

  public IPipeLineProxy<Pipeline> pipelined() {
    return null;
  }

  public void set(byte[] bytes, byte[] byteArray) {
    try {
      this.jedis.set(new String(bytes, "utf-8"), new String(byteArray, "utf-8"));
    } catch (UnsupportedEncodingException e) {}
  }

  public void expire(byte[] bytes, int cacheSeconds) {
    try {
      this.jedis.expire(new String(bytes, "utf-8"), cacheSeconds);
    } catch (UnsupportedEncodingException e) {}
  }

  public byte[] get(byte[] bytes) {
    try {
      return this.jedis.get(new String(bytes, "utf-8")).getBytes();
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  public Long hdel(String key, String... fields) {
    return this.jedis.hdel(key, fields);
  }

  public Long zadd(String key, double score, String member) {
	// TODO Auto-generated method stub
	return this.jedis.zadd(key, score, member);
  }

  public Long zadd(String key, Map<String, Double> scoreMembers) {
	// TODO Auto-generated method stub
	return this.jedis.zadd(key, scoreMembers);
  }

  public Set<String> zrange(String key, long start, long end) {
	// TODO Auto-generated method stub
	return this.jedis.zrange(key, start, end);
  }

  public Long zcard(String key) {
	// TODO Auto-generated method stub
	return this.jedis.zcard(key);
  }
  
  public Long zrem(String key,String... member) {
	// TODO Auto-generated method stub
	return this.jedis.zrem(key,member);
  }
  	
  	/**
  	 * 自增1
  	 * (方法说明描述) 
  	 *
  	 * @param key
  	 * @return 
  	 *
  	 * @see net.mycache.client.redis.model.proxy.IJedisProxy#incr(java.lang.String)
  	 */
	public Long incr(String key) {
		return this.jedis.incr(key);
	}

    public List<String> blpop(String key, int timeout) {
      return this.jedis.blpop(timeout, key);
    }

}
