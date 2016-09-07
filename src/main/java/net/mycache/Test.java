package net.mycache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.mycache.client.redis.JedisUtil;

public class Test {
	
	private static Logger logger = LoggerFactory.getLogger(Test.class);
	
	public static void main(String args[]) throws Throwable{
		
		logger.info("init");
		
		JedisUtil.addStringToJedis("aaaa","123",10000);
		
		logger.info("over");
	}
}
