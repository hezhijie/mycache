package net.mycache.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JRedisPool {
    private final Logger logger = LoggerFactory.getLogger(JRedisPool.class.getName());
	private HAConfig haConfig;
	private RouteManager routeManager;
    private final static int retryTimes = 3;
	private JedisCommands jedis;
	private static final Map<String, String> READ_COMMANDS = new HashMap<String, String>();
	private ReadPolicy readPolicy;
	
	static {
		READ_COMMANDS.put("get", "");
		READ_COMMANDS.put("exists", "");
		READ_COMMANDS.put("type", "");
		READ_COMMANDS.put("ttl", "");
		READ_COMMANDS.put("getbit", "");
		READ_COMMANDS.put("getrange", "");
		READ_COMMANDS.put("substr", "");
		READ_COMMANDS.put("hget", "");
		READ_COMMANDS.put("hmget", "");
		READ_COMMANDS.put("hexists", "");
		READ_COMMANDS.put("hlen", "");
		READ_COMMANDS.put("hkeys", "");
		READ_COMMANDS.put("hvals", "");
		READ_COMMANDS.put("hgetAll", "");
		READ_COMMANDS.put("llen", "");
		READ_COMMANDS.put("lrange", "");
		READ_COMMANDS.put("lindex", "");
		READ_COMMANDS.put("smembers", "");
		READ_COMMANDS.put("scard", "");
		READ_COMMANDS.put("sismember", "");
		READ_COMMANDS.put("srandmember", "");
		READ_COMMANDS.put("zrange", "");
		READ_COMMANDS.put("zrank", "");
		READ_COMMANDS.put("zrevrank", "");
		READ_COMMANDS.put("zrangeWithScores", "");
		READ_COMMANDS.put("zrevrangeWithScores", "");
		READ_COMMANDS.put("zcard", "");
		READ_COMMANDS.put("zscore", "");
		READ_COMMANDS.put("zcount", "");
		READ_COMMANDS.put("zrangeByScore", "");
		READ_COMMANDS.put("zrevrangeByScore", "");
		READ_COMMANDS.put("zrangeByScoreWithScores", "");
		READ_COMMANDS.put("zrevrangeByScoreWithScores", "");
	}
	
	private boolean isReadCommand(String name) {
		return READ_COMMANDS.containsKey(name);
	}
	
	public JRedisPool(final GenericObjectPoolConfig poolConfig, final String host) {
		this(poolConfig, host, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, true);
	}

	public JRedisPool(final String host) {
		this(new GenericObjectPoolConfig(), host, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, true);
	}

	public JRedisPool(final GenericObjectPoolConfig poolConfig, final String host,
			int timeout, final String password) {
		this(poolConfig, host, timeout, password, Protocol.DEFAULT_DATABASE, true);
	}

	public JRedisPool(final GenericObjectPoolConfig poolConfig, final String host,
			final int timeout) {
		this(poolConfig, host, timeout, null, Protocol.DEFAULT_DATABASE, true);
	}

	public JRedisPool(final GenericObjectPoolConfig poolConfig, final String host,
			int timeout, final String password, final int database, final boolean randomRead) {
		List<ServerInfo> infos = new ArrayList<ServerInfo>();
		int end = host.indexOf(";");
		if (end != -1) {
			String host1 = host.substring(0, end);
			int pos = host1.indexOf(":");
			if (pos != -1) {
				String ip = host1.substring(0, pos);
				String port = host1.substring(pos + 1);
				infos.add(new ServerInfo(poolConfig, ip, Integer.valueOf(port), timeout, password));			
			} else {
				logger.error("JRedis host must have port");
				System.exit(1);
			}

			host1 = host.substring(end + 1);
			pos = host1.indexOf(":");
			if (pos != -1) {
				String ip = host1.substring(0, pos);
				String port = host1.substring(pos + 1);
				infos.add(new ServerInfo(poolConfig, ip, Integer.valueOf(port), timeout, password));			
			}
		} else {
			logger.error("JRedis host must be like A.B.C.D:XXX;A.B.C.D:XXX");
			System.exit(1);
		}
		if (randomRead) {
			readPolicy = new DoubleReadPolicy();
		} else {
			readPolicy = new DoubleReadPolicy();
		}
		haConfig = new HAConfig(poolConfig, infos);
		routeManager = new RouteManager(infos);
        jedis = (JedisCommands) Proxy.newProxyInstance(JedisCommands.class.getClassLoader(), 
        		new Class[] { JedisCommands.class }, new RedisGroupInvocationHandler());
	}
	
	public void destroy() {
		if (routeManager != null) {
			routeManager.destroy();
		}
	}
	
	private Object invokeWrite(Method method, Object[] args) {
		List<RouteData> servers = routeManager.getWritableServers();
		
		if ((servers == null) || (servers.size() == 0)) {
			throw new JedisConnectionException("write failed for there are nodes is writable");
		}
		
		Object result = null;
        int e = 0;
		for (RouteData server : servers) {
			for (int i = 0; i < retryTimes; i++) {
				if (!server.isAvailable()) {
					e++;
					break;
				}
				
				try {
					result = server.invoke(method, args);
				} catch (Exception ex) {
					handleError(server);
					if (i == retryTimes - 1) {
						e++;
					} else {
						continue;
					}
				}

				break;
			}
		}
		
		if (e >= servers.size()) {
			throw new JedisConnectionException("write failed for all nodes are failed");
		}
		
		return result;
	}
		
	private void handleError(RouteData routeData) {
		routeManager.handleError(routeData);
	}

	public JedisCommands getJRedis() {
		return jedis;
	}
	
	public class RandomReadPolicy implements ReadPolicy {

		@Override
		public Object invokeRead(Method method, Object[] args) {
			Object result = null;
	        int errorCount = 0;

			while (routeManager.getReadableServers().size() > 0) {
				RouteData server = routeManager.route();
				JedisPool pool = server.getPool();
				Jedis jedis = null;
				boolean connectionError = false;
				try {
					jedis = pool.getResource();
					result = method.invoke(jedis, args);
					if(result == null){
						continue;
					}
					return result;
				} catch (Throwable t) {
	                if (t instanceof JedisConnectionException) {
	                	connectionError = true;
	                } else if (t instanceof InvocationTargetException) {
	                	try {
	                        InvocationTargetException ite = (InvocationTargetException) t;                                
	                        if (ite.getTargetException() instanceof JedisConnectionException) {
	                        	connectionError = true;
	                        }
	                	} catch (Throwable tt) {
	                		logger.warn("解包异常", tt);
	                	}
	                }
	                errorCount++;
	                if (errorCount < retryTimes) {
	                    continue;
	                }
	                
	                logger.warn("read exception: " + server.getInfo(), t);
	                
	                if (connectionError) {
	                    routeManager.onError(server);
	                } else {
	                	logger.warn("read failed for unknow exception ", t);
	                	throw new JedisConnectionException(t);
	                }
	            } finally {
	            	if (jedis != null) {
	            		if (connectionError) {
	            			pool.returnBrokenResource(jedis);
	            		} else {
	            			pool.returnResource(jedis);
	            		}
	            	}
	            }
			}
			
			throw new JedisConnectionException("JRedis read failed for all nodes are failed");
		}
		
	}
	
	public class DoubleReadPolicy implements ReadPolicy {
		@Override
		public Object invokeRead(Method method, Object[] args) {
			List<RouteData> servers = routeManager.getReadableServers();
			final int routeDataSize = servers.size();
			
			if ((servers == null) || (servers.size() == 0)) {
				throw new JedisConnectionException("read failed for there are nodes is readable");
			}
			
			Object result = null;
	        int errorCount = 0;
	        List<RouteData> selectedRouteDatas = new ArrayList<RouteData>(routeDataSize);
	        
	        for (int count = 0; count < routeDataSize; count++) {
				RouteData server = routeManager.route(selectedRouteDatas);
				selectedRouteDatas.add(server);
	        	if (!server.isAvailable()) {
	                errorCount++;
	        		continue;
	        	}
	        	
	        	try {
	        		result = server.invoke(method, args);
	        		if (result == null) {
	        			continue;
	        		}
	        		return result;
	        	} catch (Exception ex) {
	                errorCount++;
	                handleError(server);
	        	}
	        }
	        
	        if (errorCount == routeDataSize) {
    			throw new JedisConnectionException("JRedis read failed for all nodes are failed");
	        }
	        
	        return null;
		}
	}
	
    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	builder.append("{routeManager:");
    	builder.append(this.routeManager);
    	builder.append("}");
    	return builder.toString();
    }
	
    public class RedisGroupInvocationHandler implements InvocationHandler {

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (isReadCommand(method.getName())) {
				return readPolicy.invokeRead(method, args);
			} else {
				return invokeWrite(method, args);
			}
		}
    }
		
	public void dumpRandomCount() {
		for (int i = 0; i < routeManager.getRandomCount().length(); i++) {
			logger.info("random times is " + routeManager.getRandomCount().get(i));
		}
	}
}
