package net.mycache.client.local;

import java.util.HashMap;
import java.util.Map;

/**
 * 本地缓存-放在应用服务器内存中的缓存(注意集群环境中不要出现脏数据)
 * 1.2.0后不建议使用该类,请使用DynamicLocalCache与StaticLocalCache
 */
@Deprecated
public class LocalCache {
  private static Map<String, Object> localCacheListMap = new HashMap();

  public static void reBuildLocalKeyMap(Object localMap, String localCacheKey) {
    Object currentBuildObjMap = null;
    if (localCacheListMap.containsKey(localCacheKey)) {
      currentBuildObjMap = localCacheListMap.get(localCacheKey);
      synchronized (currentBuildObjMap) {
        localCacheListMap.put(localCacheKey, localMap);
      }
    }
  }

  public Map<String, Object> getLocalMapByCacheKey(String localCacheKey) {
    if (localCacheListMap.containsKey(localCacheKey)) {
      localCacheListMap.get(localCacheKey);
    }
    return null;
  }

  public void putLocalCacheKeyToMap(Object localMap, String localCacheKey) {
    if (!localCacheListMap.containsKey(localCacheKey))
      localCacheListMap.put(localCacheKey, localMap);
  }

  public static Map<String, Object> getLocalCacheMap() {
    return localCacheListMap;
  }
}
