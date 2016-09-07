package net.mycache.client.local;

import java.util.HashMap;
import java.util.Map;

/**
 * 静态本地缓存-放在应用服务器内存中的缓存(注意集群环境中不要出现脏数据)
 * 
 */
public class StaticLocalCache {
  
  private volatile static Map<String, Object> localCacheListMap = new HashMap();
  
  /**
   * 获取本地缓存Map
   * @param key key
   * @return localCacheMap value
   */
  public static Object getLocalCacheMap(String key) {
    if(localCacheListMap.containsKey(key))
      return localCacheListMap.get(key);
    return null;
  }
  
  /**
   * 构造本地缓存Map
   * @param localCache CacheMap
   */
  public static void initLocalCacheMap(Map<String, Object> localCache){
    synchronized (localCacheListMap) {
      localCacheListMap.putAll(localCache);;
    }
  }

  /**
   * 存入键值对,如果键已存在则直接返回
   * @param localMap value
   * @param localCacheKey key
   */
  public void putLocalCacheKeyToMap(Object localMap, String localCacheKey) {
    if (localCacheListMap.containsKey(localCacheKey))
      return;
    localCacheListMap.put(localCacheKey, localMap);
  }
}
