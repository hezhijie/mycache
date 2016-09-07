package net.mycache.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RouteManager {
    private final Logger logger = LoggerFactory.getLogger(RouteManager.class.getName());
	private volatile RouteSet routeSet;
    Random random = new Random();
    final Map<String, RouteData> singleCache = new HashMap<String, RouteData>();
    ExecutorService executor_retry = Executors.newSingleThreadExecutor();
    final Retry retry = new Retry();
    private static AtomicIntegerArray RANDOM_COUNT = new AtomicIntegerArray(3);
	
	public RouteManager(List<ServerInfo> infos) {
		
		logger.info("111");
		
		routeSet = createRouteSet(infos);
		startRetry();
	}

	synchronized RouteData getAtomic(ServerInfo info) {
		RouteData route = singleCache.get(info.generateKey());
		if (route == null) {
			route = new RouteData(info);
			singleCache.put(info.generateKey(), route);
		}
		
		return route;
	}
	
	private RouteSet createRouteSet(List<ServerInfo> infos) {
		List<RouteData> routes = new ArrayList<RouteData>();
		for (int i = 0; i < infos.size(); i++) {
			routes.add(getAtomic(infos.get(i)));
		}
		return new RouteSet(infos, routes);
	}
	
	
	public AtomicIntegerArray getRandomCount() {
		return RANDOM_COUNT;
	}
	
	private RouteData route(RouteSet routeSet, List<RouteData> selectedRouteDatas) {
		int[] weights = routeSet.getWeights();
        int max_random = weights[weights.length - 1];
        int preRandom = -1;
        int x = random.nextInt(max_random);
        
        for (int count = 0; count < 5; count++) {
            x = random.nextInt(max_random);
            while (x == preRandom) {
            	x = random.nextInt(max_random);
            }
            preRandom = x;
            RANDOM_COUNT.incrementAndGet(x);  
            
            for (int i = 0; i < weights.length; i++) {
                if (x < weights[i]) {
                	RouteData data = routeSet.getRoutes().get(i);
                	if ((selectedRouteDatas != null) && (selectedRouteDatas.contains(data))) {
                		break;
                	}
                	return routeSet.getRoutes().get(i);
                }
            }
        }
               
        throw new JedisConnectionException("read node is empty");
    }
	
	public RouteData route() {
		return route(routeSet, null);
	}
	
	public RouteData route(List<RouteData> selectedRouteDatas) {
		return route(routeSet, selectedRouteDatas);
	}
	
	public List<RouteData> getWritableServers() {
		return routeSet.getRoutes();
	}

	public List<RouteData> getReadableServers() {
		return routeSet.getRoutes();
	}

	private String getHostName() {
	    try {
	      InetAddress addr = InetAddress.getLocalHost();
	      return addr.getHostName().toString();// 获得机器名
	    } catch (UnknownHostException e) {
	      return "unknown hostname";
	    }
	}
	
	private String getIp() {
		String serverIp = null;
		try {
			serverIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			serverIp = "unknown ip";
		}
		return serverIp;
	}
	
	public synchronized void handleError(RouteData routeData) {
		if (!routeData.isAvailable()) {
			return;
		}
		
		routeData.handleError();
		
		if (!routeData.isAvailable()) {
			retry.addRetry(routeData);
	        logger.warn("start to detect server " + routeData.getInfo());
		}
	}
	
	public synchronized void onError(RouteData route) {
		List<ServerInfo> new_infos = (List<ServerInfo>)((ArrayList)routeSet.getInfos()).clone();
		if (new_infos.remove(route.getInfo())) {
			routeSet = createRouteSet(new_infos);
	        retry.addRetry(route);
	        logger.warn("start to detect server " + route.getInfo());
		}
	}
	
	private synchronized void handleSucces(RouteData routeData) {
		routeData.handleSuccess();
	}
	
	private synchronized void onReturn(RouteData route) {
		List<ServerInfo> new_infos = (List<ServerInfo>)((ArrayList)routeSet.getInfos()).clone();

		if (!new_infos.contains(route.getInfo())) {
			new_infos.add(route.getInfo());
		}
		routeSet = createRouteSet(new_infos);
        logger.warn("server " + route.getInfo() + " alive again");
	}
	
	 public void destroy() {
		 retry.exit = true;
		 synchronized (retry) {
			 retry.notify();
		 }
		 executor_retry.shutdownNow();
		 synchronized (singleCache) {
			 for (Map.Entry<String, RouteData> entry : singleCache.entrySet()) {
				 entry.getValue().destroy();
			 }
		 }
	 }

	 final void startRetry() {
		 executor_retry.execute(retry);
	 }
	
	private class Retry implements Runnable {
        volatile boolean exit = false;
        private final static int RETRY_INTERVAL = 1; // unit is second
        CopyOnWriteArraySet<RouteData> set = new CopyOnWriteArraySet<RouteData>();

        public void addRetry(RouteData single) {
            set.add(single);
            synchronized (Retry.this) {
                this.notify();
            }
        }

        @Override
        public void run() {
            while (!exit) {
                for (RouteData s : set) {
                	try {
                        synchronized (singleCache) {
                            RouteData route = singleCache.remove(s.getInfo().generateKey());
                            if (route != null) {
                            	route.destroy();
                            	logger.info(route.getInfo().toString() + " destory successly");
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("", e);
                    }
                	//RouteData routeData = new RouteData(s.getInfo());
                	JedisPool pool = s.createJedisPool();
                	Jedis jedis = null;
                	boolean error = true;
                    try {
                    	jedis = pool.getResource();
                    	pool.getResource().ping();
                    	synchronized (singleCache) {
                            singleCache.put(s.getInfo().generateKey(), s);
                        }
                    	error = false;
                    	s.setPool(pool);
                    	s.handleSuccess();
                        // success
                        //onReturn(routeData);
                        
                        set.remove(s);
                        logger.info(s.getInfo().toString() + " recover now");
                    } catch (Throwable t) {
                    	if (jedis != null) {
                    		pool.returnBrokenResource(jedis);
                    	}
                    	try {
                    		pool.destroy();
                        	//routeData.destroy();
                    	} catch (Exception e) {
                    	}
                    } finally {
                    	if (jedis != null && error == false) {
                			pool.returnResource(jedis);
                    	}
                    }
                }
                try {
                    synchronized (Retry.this) {
                        wait(1000 * RETRY_INTERVAL);
                    }
                } catch (InterruptedException ex) {
                    logger.warn("Retry Thread InterruptedException", ex);
                }
            }
        }
    }
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{");
		builder.append("routeSet:");
		builder.append(routeSet);
		builder.append(",retry:");
		builder.append(retry);
		builder.append("}");
		return builder.toString();
	}
}
