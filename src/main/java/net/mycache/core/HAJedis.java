package net.mycache.core;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

public class HAJedis extends Jedis {
    public HAJedis(final String host) {
    	super(host);
    }

    public HAJedis(final String host, final int port) {
    	super(host, port);
    }

    public HAJedis(final String host, final int port, final int timeout) {
    	super(host, port, timeout);
    }

    public HAJedis(JedisShardInfo shardInfo) {
    	super(shardInfo);
    }
    
}
