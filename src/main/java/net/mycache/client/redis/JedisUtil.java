package net.mycache.client.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import net.mycache.client.redis.model.proxy.IJedisProxy;
import net.mycache.client.redis.model.proxy.pipeline.IPipeLineProxy;

/**
 * Jedis操作类
 * 
 */
public class JedisUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(JedisUtil.class);
	
	/**
	 * 缓存字符串
	 * 
	 * @param key
	 * @param value
	 * @param cacheSeconds 缓存时间
	 * @return 缓存结果
	 * @throws Exception
	 */
	public static String addStringToJedis(String key, String value, int cacheSeconds) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(value))
			return null;
		IJedisProxy<?> jedis = null;
		String lastVal = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			lastVal = jedis.getSet(key, value);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : addStringToJedis success. key = {}",  key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addStringToJedis failed. key = " + key,e);
			return null;
		} finally {
			JedisConf.release(jedis);
		}
		return lastVal;
	}

	/**
	 * 缓存字符串
	 * 
	 * @param batchData
	 *            value
	 * @param cacheSeconds
	 *            缓存时间
	 * @return 缓存结果
	 * @throws Exception
	 */
	public static void addStringToJedis(Map<String, String> batchData, int cacheSeconds) {
		if (batchData.isEmpty() || batchData.size() == 0)
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			IPipeLineProxy<?> pipeline = jedis.pipelined();
			for (Map.Entry<String, String> element : batchData.entrySet()) {
				if (cacheSeconds != 0) {
					pipeline.setex(element.getKey(), cacheSeconds, element.getValue());
				} else {
					pipeline.set(element.getKey(), element.getValue());
				}
			}
			pipeline.sync();
			logger.debug("Redis : addStringToJedis success. <Map>");
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addStringToJedis failed. <Map>");
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 缓存字符串
	 * 
	 * @param batchData
	 *            value
	 * @param cacheSeconds
	 *            缓存时间
	 * @return 缓存结果
	 * @throws Exception
	 */
	public static void hincrByToJedis(String key, String incrementField, Long incrementValue, int cacheSeconds) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(incrementField) || incrementValue == null)
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			jedis.hincrBy(key, incrementField, incrementValue);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : hincrByToJedis success. key = " + key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : hincrByToJedis failed. key = " + key);
			return;
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 缓存List
	 * 
	 * @param key
	 *            key
	 * @param list
	 *            value
	 * @param cacheSeconds
	 *            缓存时间
	 * @return 缓存结果
	 * @throws Exception
	 */
	public static void addListToJedis(String key, List<String> list, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return;
		if (list != null && list.size() > 0) {
			IJedisProxy<?> jedis = null;
			try {
				jedis = JedisConf.getInstance(OPeration.WRITE);
				if (jedis.exists(key)) {
					jedis.del(key);
				}
				for (String aList : list) {
					jedis.rpush(key, aList);
				}
				if (cacheSeconds != 0) {
					jedis.expire(key, cacheSeconds);
				}
				logger.debug("Redis : addListToJedis success. key = {}", key);
			} catch (Exception e) {
				if (null != jedis)
					jedis.setBroken(true);
				logger.error("Redis : addListToJedis failed. key = {}", key);
			} finally {
				JedisConf.release(jedis);
			}
		}
	}

	/**
	 * 缓存HashMap
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            field
	 * @param value
	 *            value
	 * @param cacheSeconds
	 *            缓存时间
	 * @throws Exception
	 */
	public static void addHashMapToJedis(String key, String field, String value, int cacheSeconds) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(field) || StringUtils.isBlank(value))
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (jedis != null) {
				jedis.hset(key, field, value);
				if (cacheSeconds != 0) {
					jedis.expire(key, cacheSeconds);
				}
			}
			logger.debug("Redis : addHashMapToJedis success. key = {}", key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addHashMapToJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 更新HashMap
	 * 
	 * @param key
	 *            key
	 * @param incrementField
	 *            incrementField
	 * @param incrementValue
	 *            incrementValue
	 * @param dateField
	 *            dateField
	 * @param dateValue
	 *            dateValue
	 * @throws Exception
	 */
	public static void updateHashMapToJedis(String key, String incrementField, long incrementValue, String dateField,
			String dateValue) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(incrementField) || StringUtils.isBlank(dateField)
				|| StringUtils.isBlank(dateValue))
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			jedis.hincrBy(key, incrementField, incrementValue);
			jedis.hset(key, dateField, dateValue);
			logger.debug("Redis : updateHashMapToJedis success. key = {}", key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : updateHashMapToJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 获取缓存字符串
	 * 
	 * @param key
	 *            key
	 * @return value
	 * @throws Exception
	 */
	public static String getStringFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		String value = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis.exists(key)) {
				value = jedis.get(key);
				value = StringUtils.isNotBlank(value) && !"nil".equalsIgnoreCase(value) ? value : null;
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getStringFromJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * 获取缓存List
	 * 
	 * @param key
	 *            key
	 * @return list
	 * @throws Exception
	 */
	public static List<String> getListFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		List<String> list = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis.exists(key)) {
				list = jedis.lrange(key, 0, -1);
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getListFromJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
		return list;
	}

	/**
	 * 获取缓存HashMap
	 * 
	 * @param key
	 *            key
	 * @return hashmap
	 * @throws Exception
	 */
	public static Map<String, String> getHashMapFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		Map<String, String> hashMap = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			hashMap = jedis.hgetAll(key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getHashMapFromJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
		return hashMap;

	}

	/**
	 * 获取HahMap制定域
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            fieldname
	 * @return value
	 * @throws Exception
	 */
	public static String getHashMapValueFromJedis(String key, String field) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(field))
			return null;
		String value = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.hget(key, field);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getHashMapValueFromJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * 删除某db的某个key值
	 * 
	 * @param key
	 *            key
	 * @return delResult
	 * @throws Exception
	 */
	public static Long delKeyFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			Long del = jedis.del(key);
			return del;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : delKeyFromJedis failed. key = {}", key);
			return null;
		} finally {
			JedisConf.release(jedis);
		}

	}

	/**
	 * 是否存在key
	 * 
	 * @param key
	 *            key
	 * @return boolean
	 * @throws Exception
	 */
	public static boolean existKey(String key) {
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			Boolean exists = jedis.exists(key);
			return exists;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : existKey failed. key = {}", key);
			return false;
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 缓存对象
	 * 
	 * @param key
	 *            key
	 * @param object
	 *            object
	 * @param cacheSeconds
	 *            缓存时间
	 * @throws Exception
	 */
	public static void addObject(String key, Serializable object, int cacheSeconds) {
		IJedisProxy<?> jedis = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			oos = new ObjectOutputStream(bos);
			oos.writeObject(object);
			jedis.set(key.getBytes(), bos.toByteArray());
			if (cacheSeconds != 0) {
				jedis.expire(key.getBytes(), cacheSeconds);
			}
			logger.debug("Redis : addObject success. key = {}", key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addObject failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
			try {
				if (null != oos)
					oos.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * 获取缓存对象
	 * 
	 * @param key
	 *            key
	 * @return object
	 * @throws Exception
	 */
	public static Object getObject(final String key) {
		if (null == key)
			return null;
		IJedisProxy<?> jedis = null;
		Object object = null;
		ObjectInputStream ois = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			ByteArrayInputStream bais = new ByteArrayInputStream(jedis.get(key.getBytes()));
			ois = new ObjectInputStream(bais);
			object = ois.readObject();
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getObject failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
			try {
				if (null != ois)
					ois.close();
			} catch (Exception e) {
			}
		}
		return object;
	}

	/**
	 * 缓存HashMap
	 * 
	 * @param key
	 *            key
	 * @param map
	 *            map
	 * @param cacheSeconds
	 *            缓存时间
	 * @throws Exception
	 */
	public static void addHashMapToJedis(String key, Map<String, String> map, int cacheSeconds) {
		if (StringUtils.isBlank(key) || map.isEmpty() || map.size() == 0)
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (jedis != null) {
				jedis.hmset(key, map);
				if (cacheSeconds != 0) {
					jedis.expire(key, cacheSeconds);
				}
			}
			logger.debug("Redis : addHashMapToJedis success. key = {}", key);
		} catch (Exception e) {
			logger.error("Redis write 失败", e);
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addHashMapToJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 获取HahMap制定域
	 * 
	 * @param key
	 *            key
	 * @return value
	 * @throws Exception
	 */
	public static Map<String, String> getAllMapValueFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		Map<String, String> value = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.hgetAll(key);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getAllMapValueFromJedis failed. key = {}", key);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * 获取HahMap制定域
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            fieldname
	 * @return value
	 * @throws Exception
	 */
	public static List<String> getMapListValueFromJedis(String key, String... field) {
		if (StringUtils.isBlank(key) || field.length == 0)
			return null;
		List<String> value = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.hmget(key, field);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getMapListValueFromJedis failed. key = {} , fields = {}",key,field);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * 更新缓存时间
	 * 
	 * @param key
	 *            key
	 * @param cacheSeconds
	 *            缓存时间
	 */
	public static void updateCacheSeconds(String key, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : updateCacheSeconds success. key = {}", key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : updateCacheSeconds failed. key = {}" , key , e);
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * 删除HahMap制定域
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            fieldname
	 * @return value 删除成功field个数
	 * @throws Exception
	 */
	public static Long deleteMapFieldsFromJedis(String key, String... field) {
		if (StringUtils.isBlank(key) || field.length == 0)
			return null;
		Long value = null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.hdel(key, field);
				}
			}
			logger.debug("Redis : deleteMapFieldsFromJedis hit. key = {} , fields = {}", key,  field);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : deleteMapFieldsFromJedis failed. key = "+key+" , field = "+field, e);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * @param key
	 *            集合对应key值。
	 * @param score
	 *            需要缓存到集合中的对象对应的分值，该值用于进行内部排序使用
	 * @param member
	 *            需要缓存到集合中的对象
	 * @return Long
	 *         当集合中存在该member值时，会直接覆盖掉原来的member，并返回0。当member为一个新增的对象时，并且成功写入到集合中，则返回1。
	 */
	public static Long addMemberToSortedSet(String key, double score, String member, int cacheSeconds) {
		if (StringUtils.isBlank(key) || StringUtils.isBlank(member))
			return null;
		IJedisProxy<?> jedis = null;
		Long value = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (jedis != null) {
				value = jedis.zadd(key, score, member);
				if (cacheSeconds != 0) {
					jedis.expire(key, cacheSeconds);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addMemberToSortedSet failed. key = " + key + ". score =" + score + ". fields = " + member);
		} finally {
			JedisConf.release(jedis);
		}

		return value;
	}

	/**
	 * @param key
	 *            集合对应key值。
	 * @param scoreMembers
	 *            需要缓存到集合中的map对象。
	 * @return Long
	 *         当集合中存在该member值时，会直接覆盖掉原来的member，并返回0。当member为一个新增的对象时，并且成功写入到集合中，则返回1。
	 */
	public static Long addSortedSetToRedis(String key, Map<String, Double> scoreMembers, int cacheSeconds) {
		if (StringUtils.isBlank(key) || scoreMembers == null)
			return null;
		IJedisProxy<?> jedis = null;
		Long value = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (jedis != null) {
				value = jedis.zadd(key, scoreMembers);
				if (cacheSeconds != 0) {
					jedis.expire(key, cacheSeconds);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addSortedSetToRedis failed. key = " + key + ". scoreMembers ="
					+ JSON.toJSONString(scoreMembers));
		} finally {
			JedisConf.release(jedis);
		}

		return value;
	}

	public static Set<String> getSortedSetFromRedis(String key, long start, long end) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		Set<String> value = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.zrange(key, start, end);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error(
					"Redis : getSortedSetFromRedis failed. key = " + key + ". start =" + start + ".end=" + end);
		} finally {
			JedisConf.release(jedis);
		}

		return value;
	}

	/**
	 * 获取指定key对应的set集合中对象的数量
	 * 
	 * @param key
	 * @return
	 */
	public static Long getSortedSetCountFromRedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		Long value = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.zcard(key);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : getSortedSetCountFromRedis failed. key = " + key);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * 删除指定key的缓存中排序set集合中的一个或者多个元素
	 * 
	 * @param key
	 * @param members
	 * @return
	 */
	public static Long removeSortedSetMembersFromRedis(String key, String... members) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		Long value = null;

		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			if (jedis != null) {
				if (jedis.exists(key)) {
					value = jedis.zrem(key, members);
				}
			}
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : removeSortedSetMembersFromRedis failed. key = " + key + "members = " + members);
		} finally {
			JedisConf.release(jedis);
		}
		return value;
	}

	/**
	 * 缓存字符串
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @param cacheSeconds
	 *            缓存时间
	 * @return 缓存结果
	 * @throws Exception
	 */
	public static Long incrementToJedis(String key, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		long lastVal = 0L;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			lastVal = jedis.incr(key);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : incrementToJedis success. key = " + key + "; result=" + lastVal);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : addStringToJedis failed. key = " + key + "; cause:" + e.getMessage());
			return null;
		} finally {
			JedisConf.release(jedis);
		}
		return lastVal;
	}

	/**
	 * 缓存List
	 * 
	 * @param key
	 *            key
	 * @param list
	 *            value
	 * @param cacheSeconds
	 *            缓存时间
	 * @return 缓存结果
	 * @throws Exception
	 */
	public static void rpushValueToJedis(String key, String value, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			jedis.rpush(key, value);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : rpushValueToJedis success. key = " + key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : rpushValueToJedis failed. key = " + key);
		} finally {
			JedisConf.release(jedis);
		}
	}

	public static String lpopFromJedis(String key, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			String popValue = jedis.lpop(key);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : lpopFromJedis success. key = " + key);
			return popValue;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : lpopFromJedis failed. key = " + key);
			return null;
		} finally {
			JedisConf.release(jedis);
		}

	}

	public static void lpushValueToJedis(String key, String value, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			jedis.lpush(key, value);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.debug("Redis : lpushValueToJedis success. key = " + key);
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : lpushValueToJedis failed. key = " + key);
		} finally {
			JedisConf.release(jedis);
		}
	}

	public static String rpopFromJedis(String key, int cacheSeconds) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			String popValue = jedis.rpop(key);
			if (cacheSeconds != 0) {
				jedis.expire(key, cacheSeconds);
			}
			logger.error("Redis : rpopFromJedis success. key = " + key);
			return popValue;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : rpopFromJedis failed. key = " + key);
			return null;
		} finally {
			JedisConf.release(jedis);
		}
	}

	public static Long llenFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.READ);
			Long len = jedis.llen(key);
			logger.debug("Redis : llenFromJedis success. key = " + key);
			return len;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : llenFromJedis failed. key = " + key);
			return null;
		} finally {
			JedisConf.release(jedis);
		}
	}

	public static String lpopFromJedis(String key) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			String popValue = jedis.lpop(key);
			logger.debug("Redis : lpopFromJedis success. key = " + key);
			return popValue;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : lpopFromJedis failed. key = " + key);
			return null;
		} finally {
			JedisConf.release(jedis);
		}
	}

	/**
	 * LPOP命令的阻塞版本
	 * 
	 * @Title: blpopFromJedis
	 * @param arg
	 * @return List<String>
	 */
	public static List<String> blpopFromJedis(String key, int timeout) {
		if (StringUtils.isBlank(key))
			return null;
		IJedisProxy<?> jedis = null;
		try {
			jedis = JedisConf.getInstance(OPeration.WRITE);
			List<String> popValue = jedis.blpop(key, timeout);
			logger.debug("Redis : blpopFromJedis success. key = " + key);
			return popValue;
		} catch (Exception e) {
			if (null != jedis)
				jedis.setBroken(true);
			logger.error("Redis : blpopFromJedis failed. key = " + key);
			return null;
		} finally {
			JedisConf.release(jedis);
		}
	}

}
