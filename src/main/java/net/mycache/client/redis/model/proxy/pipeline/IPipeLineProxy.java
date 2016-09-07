package net.mycache.client.redis.model.proxy.pipeline;

import redis.clients.jedis.Queable;

/**
 * 管道代理抽象接口--抽象管道代理行为
 */
public interface IPipeLineProxy<M extends Queable> {
    
    public void setPipeLine(M pipeLine);

    public M getPipeLine();

    public void setex(String key, int cacheSeconds, String value);

    public void set(String key, String value);

    public void sync();
  }