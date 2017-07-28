package sea.redis.lua;

import redis.clients.jedis.Jedis;
import shade.storm.org.apache.commons.lang.StringUtils;

public class MiaoSha {

	String pName = "good_stores";
	int maxCount = 50;

	public void generateTestData() {
		Jedis client = RedisHelper.getRedisClient();
		Long curentLength=client.llen(pName);
		client.del(pName);
		for (int i = 0; i < maxCount-curentLength; i++) {
			client.lpush(pName, i+"");
		}
		
		System.out.println(client.llen(pName));
		client.close();
	}

	 public void shop() {
		 Jedis client = RedisHelper.getRedisClient();
		for (int i = 0; i < maxCount*20; i++) {
			String value= client.lpop(pName);
			if(StringUtils.isNotBlank(value)){
			System.out.println("pop:"+value);
			}
		}
		
	}
	 
	 public void testPipeLine(){
		 long start=System.currentTimeMillis();
		 Jedis client = RedisHelper.getRedisClient();
		 /*Pipeline p= client.pipelined();
		 for(int i=0;i<1000000;i++){
		 p.set("test", "1");
		 p.get("test");
		 }
		 p.sync();*/
		 
		 for(int i=0;i<10000;i++){
			 client.set("test", "1");
			 }
		 System.out.println("time:"+(System.currentTimeMillis()-start));
	 }

	public static void main(String[] args) throws InterruptedException {
		final MiaoSha sha=new MiaoSha();
		sha.generateTestData();
		for(int i=0;i<1000;i++){
			new Thread(new Runnable(){

				@Override
				public void run() {
					sha.shop();
				}
				
			});
		}
		sha.shop();
		//sha.testPipeLine();
	}
}
