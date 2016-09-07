package net.mycache.client.redis.pool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import net.mycache.client.redis.model.proxy.IJedisProxy;
import net.mycache.client.redis.pool.proxy.IBasePoolProxy;

public class PoolProxyFactory {

  /**
   * 反射创建PoolProxy实例
   * @param className 实例类名
   * @param poolConfig config配置
   * @param timeOut 超时时间
   * @param hostIp host/port
   * @return PoolProxy实例
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public IBasePoolProxy<IJedisProxy<?>> createPoolProxy(String className,
                                                        GenericObjectPoolConfig poolConfig,
                                                        Integer timeOut,
                                                        String hostIp)
          throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                IllegalArgumentException, InvocationTargetException,
                NoSuchMethodException, SecurityException {

    Class<IBasePoolProxy> poolProxy = (Class<IBasePoolProxy>) Class.forName(className);
    Class[] paramTypes = {GenericObjectPoolConfig.class, Integer.class, String.class};
    Object[] params = {poolConfig, timeOut, hostIp};
    Constructor<IBasePoolProxy> poolProxyCons = poolProxy.getConstructor(paramTypes);
    return poolProxyCons.newInstance(params);
  }

}
