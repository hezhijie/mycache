package net.mycache.client.redis.model.proxy.pipeline;

import redis.clients.jedis.ShardedJedisPipeline;

/**
 * ShardedPipeLine代理类
 */
public class ShardedPipeLineProxy implements IPipeLineProxy<ShardedJedisPipeline>{
  
  private ShardedJedisPipeline pipeLine;
  
  public ShardedPipeLineProxy(){
  }
  
  public ShardedPipeLineProxy(ShardedJedisPipeline pipeLine){
    this.pipeLine = pipeLine;
  }

  public void setPipeLine(ShardedJedisPipeline pipeLine) {
    this.pipeLine = pipeLine;
  }

  public ShardedJedisPipeline getPipeLine() {
    return this.pipeLine;
  }

  public void setex(String key, int cacheSeconds, String value) {
    this.pipeLine.setex(key, cacheSeconds, value);
  }

  public void set(String key, String value) {
    this.pipeLine.set(key, value);
  }

  public void sync() {
    this.pipeLine.sync();
  }

}
