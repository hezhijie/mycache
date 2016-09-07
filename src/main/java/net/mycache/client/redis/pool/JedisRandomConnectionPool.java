package net.mycache.client.redis.pool;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import net.mycache.client.pool.JedisMarkPool;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * jedisPool随机连接池-twemproxy/twemproxy-agent/sentinel集群方案连接池
 */
public class JedisRandomConnectionPool {

  /**
   * 构造方法
   * @param startNodes 节点信息Set
   * @param poolConfig 池config信息
   * @param timeOut 超时时间
   */
  public JedisRandomConnectionPool(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig,
      Integer timeOut) {
    if (null == startNodes || startNodes.isEmpty() || startNodes.size() == 0)
      throw new RuntimeException("[JedisRandomConnectionPool Init Error:Host Info Can't Be Null.]");
    if (null == poolConfig)
      throw new RuntimeException("[JedisRandomConnectionPool Init Error:PoolConfig Can't Be Null.]");
    if (null == timeOut)
      throw new RuntimeException("[JedisRandomConnectionPool Init Error:TimeOut Can't Be Null.]");
    this.startNodes = startNodes;
    this.poolConfig = poolConfig;
    this.timeOut = timeOut;
    try {
      initialize();
    } catch (Exception e) {
      throw new RuntimeException("[JedisRandomConnectionPool Init Error:Init Failed.]");
    }
  }

  /**节点信息Set*/
  private Set<HostAndPort> startNodes;
  /**poolConfig信息*/
  private GenericObjectPoolConfig poolConfig;
  /**超时时间*/
  private Integer timeOut;

  /**poolList副本*/
  private ThreadLocal<List<JedisMarkPool>> jedispools = new ThreadLocal<List<JedisMarkPool>>();

  /**scduler调度器*/
  private Timer timer = new Timer(true);

  /**调度周期*/
  private Long delay = 100l;  //TODO 参数藏的比较深， 应该从外层的配置对象中去， 现在这样参数遍布的比较零散  

  /**
   * 获取可用Jedis客户端
   * @return 可用Jedis客户端
   */
  public Jedis getConnection() {
    List<JedisMarkPool> pools = getShuffledNodesPool();
    return getRandomConnection(pools);
  }

  /**
   * 随机获取一个客户端
   * @param pools poolList
   * @return 可用客户端
   */
  private Jedis getRandomConnection(List<JedisMarkPool> pools) {
    if (null == pools || pools.size() == 0) {
      throw new RuntimeException("[JedisRandomConnectionPool GetConn Error:NaN Jedis Can Use.]");
    }
    Jedis jedis = null;
    int firstNode = new Random().nextInt(pools.size());
    JedisMarkPool jedisMarkPool = pools.get(firstNode);
    try {
      jedis = jedisMarkPool.getResource();
      if (null == jedis) {
        pools.remove(firstNode);
        jedisMarkPool.fail();
        return getRandomConnection(pools);
      }
    } catch (Exception e) {
      if (null != jedis) jedisMarkPool.returnBrokenResource(jedis);
      pools.remove(firstNode);
      jedisMarkPool.fail();
      return getRandomConnection(pools);
    }
    return jedis;
  }

  /**
   * 获取随机顺序poolList
   * @return 随机排序poolList
   */
  private List<JedisMarkPool> getShuffledNodesPool() {
    List<JedisMarkPool> pools = jedispools.get();
    if (null == pools || pools.size() != nodes.size()) {
      pools = Lists.newArrayList();
      pools.addAll(nodes.values());
      jedispools.set(pools);
      Collections.shuffle(pools);
    }
    return pools;
  }

  /**
   * 初始化方法
   * 1.清空节点Map
   * 2.遍历hostList,添加host节点
   * 3.开启schedule调度,监控host状态
   */
  private void initialize() {
    this.nodes.clear();
    this.guard.clear();
    for (HostAndPort hostAndPort : startNodes) {
      JedisMarkPool jp =
          new JedisMarkPool(poolConfig, hostAndPort.getHost(), hostAndPort.getPort(), timeOut);

      Jedis jedis = null;
      try {
        jedis = jp.getResource();
        if(null!=jedis){
          jp.returnResource(jedis);
          nodes.put(jp.getNodekey(), jp);
          jedis=null;
        }
        break;
      } catch (JedisConnectionException e) {
        if (jedis != null) {
          jp.returnBrokenResource(jedis);
          jedis = null;
        }
        guard.put(jp.getNodekey(), jp);
      } finally {
        if (jedis != null) {
          jp.returnResource(jedis);
          jedis = null;
        }
      }
    }

    timer.schedule(new LoopConnection(this), 0, delay);
  }

  /**可用node节点*/
  protected Map<String, JedisMarkPool> nodes = Maps.newConcurrentMap();

  /**不可用node节点*/
  protected Map<String, JedisMarkPool> guard = Maps.newConcurrentMap();

  /**
   * 释放客户端连接
   * @param connection 客户端连接
   */
  public void returnConnection(Jedis connection) {
    nodes.get(getNodeKey(connection.getClient())).returnResource(connection);
  }

  /**
   * 释放损坏的客户端连接
   * @param connection 已损坏的客户端连接
   */
  public void returnBrokenConnection(Jedis connection) {
    nodes.get(getNodeKey(connection.getClient())).returnBrokenResource(connection);
  }

  /**
   * 获取可用节点
   * @return 可用节点Map
   */
  public Map<String, JedisMarkPool> getNodes() {
    return nodes;
  }

  /**
   * 获取指定客户端nodeKey
   * @param client 客户端 
   * @return nodeKey
   */
  protected String getNodeKey(Client client) {
    return client.getHost() + ":" + client.getPort();
  }

  /**
   * 轮询挂起不可用节点
   */
  public void loopNodes() {
    for (JedisMarkPool pool : nodes.values()) {
      if (pool.isConnect()) continue;
      String nodekey = pool.getNodekey();
      if (!guard.containsValue(pool)) guard.put(nodekey, pool);
      nodes.remove(nodekey);
    }
  }

  /**
   * 轮询重启可用节点
   */
  public void loopGuard() {
    for (JedisMarkPool pool : guard.values()) {
      if (pool.connect()) {
        String nodekey = pool.getNodekey();
        if (!nodes.containsValue(pool)) nodes.put(nodekey, pool);
        guard.remove(nodekey);
      }
    }
  }

  /**
   * JedisRandomConnectionPool轮询任务类
   * @author <a href="mailto:dan.zhao@elong.corp.com">zhaodan</a>
   * @version 1.0
   * @date 2014年6月6日
   */
  class LoopConnection extends TimerTask {

    private JedisRandomConnectionPool pool;

    public LoopConnection(JedisRandomConnectionPool jedisRandomConnectionPool) {
      this.pool = jedisRandomConnectionPool;
    }

    @Override
    public void run() {
      pool.loopNodes();
      pool.loopGuard();
    }

  }

}
