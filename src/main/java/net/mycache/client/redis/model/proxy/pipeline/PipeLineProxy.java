package net.mycache.client.redis.model.proxy.pipeline;

import redis.clients.jedis.Pipeline;

/**
 * Pipeline代理类
 */
public class PipeLineProxy implements IPipeLineProxy<Pipeline> {
  
  private Pipeline pipeLine;

  public PipeLineProxy(Pipeline pipelined) {
    this.pipeLine = pipelined;
  }

  public void setPipeLine(Pipeline pipeLine) {
    this.pipeLine = pipeLine;
  }

  public Pipeline getPipeLine() {
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
