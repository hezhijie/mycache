package net.mycache.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RouteData {
    private final Logger logger = LoggerFactory.getLogger(RouteData.class.getName());
	private ServerInfo info;
	private JedisPool pool;
	private ServerRuntimeInfo runtimeInfo;

	public RouteData(ServerInfo info) {
		this(info, new JedisPool(info.getPoolConfig(), 
				info.getHost(), info.getPort(), 
				info.getTimeout(), info.getPassword()));
	}
	
	public RouteData(ServerInfo info, JedisPool pool) {
		this.info = info;
		this.pool = pool;
		this.runtimeInfo = new ServerRuntimeInfo();
	}
	
	public Object invoke(Method method, Object[] args) {
		if (!runtimeInfo.isAvailable()) {
			throw new RuntimeException(info.toString() + " is not available");
		}
		
		Object result = null;
		boolean broken = false;
		Jedis jedis = pool.getResource();
		try {
			result = method.invoke(jedis, args);
		} catch (Exception e) {
			if (e instanceof JedisConnectionException) {
            	broken = true;
            } else if (e instanceof InvocationTargetException) {
            	try {
                    InvocationTargetException ite = (InvocationTargetException) e;                                
                    if (ite.getTargetException() instanceof JedisConnectionException) {
                    	broken = true;
                    }
            	} catch (Throwable tt) {
            		logger.warn("解包异常", tt);
            	}
            }
			throw new RuntimeException(e);
		} finally {
			if (jedis != null) {
				if (broken) {						
					pool.returnBrokenResource(jedis);
				} else {
					pool.returnResource(jedis);
				}					
			}
		}
		
		return result;
	}
	
	public JedisPool createJedisPool() {
		return new JedisPool(info.getPoolConfig(), 
				info.getHost(), info.getPort(), 
				info.getTimeout(), info.getPassword());
	}
	
	public synchronized boolean isAvailable() {
		return this.runtimeInfo.isAvailable();
	}
	
	public synchronized void handleError() {
		if (!runtimeInfo.isAvailable()) {
			return;
		}
		
		runtimeInfo.incrFailedTimes();
		if (runtimeInfo.isNeedDetect()) {
			runtimeInfo.setUnavailable();
		}
	}
	
	public synchronized void handleSuccess() {
		runtimeInfo.reset();
	}
	
	public void destroy() {
		if (pool != null) {
			pool.destroy();
		}
	}
	
	public ServerInfo getInfo() {
		return info;
	}
	
	public void setInfo(ServerInfo info) {
		this.info = info;
	}
	
	public JedisPool getPool() {
		return pool;
	}
	
	public void setPool(JedisPool pool) {
		this.pool = pool;
	}
	
	
	@Override
	public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        
        if (getClass() != obj.getClass()) {
            return false;
        }
        
        final RouteData route = (RouteData) obj;
        
        if (this.info.getHost().equals(route.getInfo().getHost()) 
        		&& this.info.getPort() == route.getInfo().getPort()) {
        	return true;
        }
        
        return false;
    }
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{serverInfo:");
		builder.append(this.info);
		builder.append(",jedisPool:{numActive:");
		builder.append(pool.getNumActive());
		builder.append(",numIdle:");
		builder.append(pool.getNumIdle());
		builder.append("}");
		builder.append(",serverRuntimeInfo:");
		builder.append(this.runtimeInfo);
    	builder.append("}");
		return builder.toString();
	}
}
