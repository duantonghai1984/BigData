package sea.redis.lua;

import redis.clients.jedis.Jedis;

public class RedisHelper {
	static String host = "127.0.0.1";
	static int port=6379;
	
	
	public static Jedis getRedisClient(){
		Jedis jedis = new Jedis(host,port);
		return jedis;
	}

}
