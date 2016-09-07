package net.mycache.client.model;

/**
 * 负载节点 别名-值 对象
 * 
 */
public class NodeMap<T> {

  /**
   * 节点别名
   */
  private String nodename;

  /**
   * 负载节点
   */
  private T node;

  public NodeMap(String nodename, T node) {
    this.nodename = nodename;
    this.node = node;
  }

  public String getNodename() {
    return nodename;
  }

  public void setNodename(String noodname) {
    this.nodename = noodname;
  }

  public T getNode() {
    return node;
  }

  public void setNode(T node) {
    this.node = node;
  }

}
