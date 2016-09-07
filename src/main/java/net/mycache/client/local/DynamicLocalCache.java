package net.mycache.client.local;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态本地缓存-放在应用服务器内存中的缓存(注意集群环境中不要出现脏数据)
 */
public class DynamicLocalCache {
  private static Map<String, Object> localCacheListMap = new ConcurrentHashMap<String, Object>();

  /**
   * 更新已存在键值对
   * @param localMap value
   * @param localCacheKey key
   */
  public static void reBuildLocalKeyMap(Object localMap, String localCacheKey) {
    if (localCacheListMap.containsKey(localCacheKey)) {
       localCacheListMap.put(localCacheKey, localMap);
    }
  }

  /**
   * 获取缓存
   * @param localCacheKey key
   * @return value
   */
  public Object getLocalMapByCacheKey(String localCacheKey) {
    if (localCacheListMap.containsKey(localCacheKey)) {
      return localCacheListMap.get(localCacheKey);
    }
    return null;
  }

  /**
   * 存入键值对,如果键已存在可选择是否更新键值对
   * @param localMap value
   * @param localCacheKey key
   * @param rebuild 是否更新
   */
  public void putLocalCacheKeyToMap(Object localMap, String localCacheKey, boolean rebuild) {
    if (!localCacheListMap.containsKey(localCacheKey)){
      localCacheListMap.put(localCacheKey, localMap);
    }else if(rebuild){
      reBuildLocalKeyMap(localMap, localCacheKey);
    }
  }

  /**
   * 获取本地缓存Map
   * @return localCacheMap
   */
  public static Map<String, Object> getLocalCacheMap() {
    return localCacheListMap;
  }
}
