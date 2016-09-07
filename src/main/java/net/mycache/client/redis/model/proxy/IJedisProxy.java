package net.mycache.client.redis.model.proxy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.JedisCommands;

import net.mycache.client.redis.model.proxy.pipeline.IPipeLineProxy;

/**
 * Jedis实例代理
 */
public interface IJedisProxy<T extends JedisCommands> {

	public boolean isBroken();

	public void setBroken(boolean broken);

	public T getJedis();

	public void setJedis(T jedis);

	public String getSet(String key, String value);

	public void expire(String key, int cacheSeconds);

	public IPipeLineProxy<?> pipelined();

	public boolean exists(String key);

	public Long del(String key);

	public void rpush(String key, String aList);
	
	public String lpop(String key);
	
    public void lpush(String key, String aList);
	    
	public String rpop(String key);
	
	public Long llen(String key);

	public void hset(String key, String field, String value);

	public void hincrBy(String key, String incrementField, long incrementValue);

	public String get(String key);

	public List<String> lrange(String key, int i, int j);

	public Map<String, String> hgetAll(String key);

	public String hget(String key, String field);

	public void set(byte[] bytes, byte[] byteArray);

	public void expire(byte[] bytes, int cacheSeconds);

	public byte[] get(byte[] bytes);

	public void hmset(String key, Map<String, String> map);

	public List<String> hmget(String key, String... field);

	public Long hdel(String key, String... fields);
	
	/**
	 * key对应的value+1
	 * (方法说明描述) 
	 *
	 * @param key
	 * @return
	 */
	public Long incr(String key);

	/**
	 * 
	 * 向指定key的redis缓存中加入member
	 * 
	 * @param key
	 * @param score
	 *            成员分数，内部排序按照score进行排序
	 * @param member
	 *            新加入的成员
	 * @return 返回成功插入的个数，0表示没有插入，1表示插入成功。
	 */
	public Long zadd(String key, double score, String member);

	/**
	 * 向指定key的redis缓存中加入Map对象
	 * 
	 * @param key
	 * @param scoreMembers
	 *            key为member，value为score值，参见 zadd(String key, double score,
	 *            String member);
	 * @return 返回当前成功插入的对象的个数
	 */
	public Long zadd(String key, Map<String, Double> scoreMembers);

	/**
	 * 从redis中获取end-start个对象
	 * 
	 * @param key
	 * @param start
	 *            起始id
	 * @param end
	 *            结束id
	 * @return Set<String> 当end=-1时，返回所有的结果。当end不为负数时，返回end-start+1个member结果。
	 */
	public Set<String> zrange(String key, long start, long end);

	/**
	 * 获取指定key的set集合中对象的数量
	 * 
	 * @param key
	 * @return
	 */
	public Long zcard(String key);

	/**
	 * 删除指定key下的set集合中一个或者member元素
	 * @param key
	 * @param member
	 * @return 成功删除的元素的数量
	 */
	public Long zrem(String key, String... member);
	
    List<String> blpop(String key, int timeout);

}
