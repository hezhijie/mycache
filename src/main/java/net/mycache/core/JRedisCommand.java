package net.mycache.core;

import java.util.HashMap;
import java.util.Map;

public class JRedisCommand {
	private static final Map<String, String> READ_COMMANDS = new HashMap<String, String>();

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
	
	public static boolean isReadCommand(String name) {
		return READ_COMMANDS.containsKey(name);
	}
}
