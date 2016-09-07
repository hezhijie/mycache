package net.mycache.client.pool;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import net.mycache.client.model.NodeMap;
import com.google.common.hash.HashFunction;

/**
 * 一致性哈希池
 */
public class ConsistentHashPool<T> {

  /**
   * 哈希算法
   */
  private final HashFunction hashFunction;
  /**
   * 哈希环粒度
   */
  private final int numberOfReplicas;
  /**
   * 编码集
   */
  private final Charset charset;
  /**
   * 哈希池
   */
  private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();

  public HashFunction getHashFunction() {
    return hashFunction;
  }

  public int getNumberOfReplicas() {
    return numberOfReplicas;
  }

  public Charset getCharset() {
    return charset;
  }

  public SortedMap<Integer, T> getCircle() {
    return circle;
  }

  public ConsistentHashPool(HashFunction hashFunction, int numberOfReplicas, String charset,
      String nodenames) {
    this(hashFunction, numberOfReplicas, charset);
  }

  /**
   * 构造方法
   * 
   * @param hashFunction 哈希算法
   * @param numberOfReplicas 哈希环粒度
   * @param charset 字符集
   * @param nodes 节点集合
   */
  public ConsistentHashPool(HashFunction hashFunction, int numberOfReplicas, String charset,
      Collection<NodeMap<T>> nodes) {
    this(hashFunction, numberOfReplicas, charset);

    for (NodeMap<T> node : nodes) {
      add(node.getNodename(), node.getNode());
    }
  }

  /**
   * 私有构造方法
   * 
   * @param hashFunction 哈希算法
   * @param numberOfReplicas 哈希环粒度
   * @param charset 字符集
   */
  private ConsistentHashPool(HashFunction hashFunction, int numberOfReplicas, String charset) {
    this.hashFunction = hashFunction;
    this.numberOfReplicas = numberOfReplicas;
    Charset forName = Charset.forName(charset);
    if (null != forName)
      this.charset = forName;
    else
      this.charset = Charset.forName("UTF-8");
  }

  /**
   * 增加节点
   * 
   * @param nodename 节点别名
   * @param node 节点
   */
  public void add(String nodename, T node) {
    for (int i = 0; i < numberOfReplicas; i++) {
      circle.put(hashFunction.hashString(nodename + i, charset).asInt(), node);
    }
  }

  /**
   * 删除节点
   * 
   * @param nodename 节点别名
   */
  public void remove(String nodename) {
    for (int i = 0; i < numberOfReplicas; i++) {
      circle.remove(hashFunction.hashString(nodename + i, charset).asInt());
    }
  }

  /**
   * 获取key所在节点
   * 
   * @param key key
   * @return 所在节点
   */
  public T get(String key) {
    if (circle.isEmpty()) {
      return null;
    }
    int hash = hashFunction.hashString(key, charset).asInt();
    if (!circle.containsKey(hash)) {
      SortedMap<Integer, T> tailMap = circle.tailMap(hash);
      hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }
    return circle.get(hash);
  }

}
