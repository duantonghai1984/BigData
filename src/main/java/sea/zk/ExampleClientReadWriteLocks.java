package sea.zk;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;


public class ExampleClientReadWriteLocks {
	private static final String ZK_ADDRESS = "localhost:2181";
    private static final String ZK_LOCK_PATH = "/zktest/lock0";
 
    /**
     * 下面的程序会启动几个线程去争夺锁，拿到锁的线程会占用5秒
     */
    public static void main(String[] args) throws InterruptedException {
        // 1.Connect to zk
        CuratorFramework client = null/*CuratorFrameworkFactory.newClient(ZK_ADDRESS,null)*/;
        client.start();
 
        System.out.println(client.getState());
 
        System.out.println("zk client start successfully!");
 
        InterProcessMutex lock = new InterProcessMutex(client, ZK_LOCK_PATH);
 
        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                doWithLock(client, lock);
            }, "Thread-" + i).start();
        }
 
    }
 
    private static void doWithLock(CuratorFramework client, InterProcessMutex lock) {
        try {
            String name = Thread.currentThread().getName();
            if (lock.acquire(10 * 1000, TimeUnit.SECONDS)) {
 
                System.out.println(name + " hold lock");
 
                System.out.println(client.getChildren().forPath(ZK_LOCK_PATH));
 
                Thread.sleep(5000L);
                System.out.println(name + " release lock");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
