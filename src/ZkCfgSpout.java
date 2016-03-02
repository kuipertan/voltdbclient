import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ZkCfgSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private CuratorFramework client;
	private ExecutorService pool;
	private  Lock lock;
	private  Condition connProduce ;
	private boolean firstStart;
	private String configPath;
	private String configZks;
	
	public ZkCfgSpout(String zk){
		configZks = zk;
	}
	
	public void nextTuple() {
		// TODO Auto-generated method stub
		if (firstStart){
			firstStart = false;
		}
		else{
			 lock.lock();                                
			 try {
				connProduce.await();
			 } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			 } 
			 finally{
				 lock.unlock();
			 }
		}
		
		// get data and send
		//new String(nodeCache.getCurrentData().getData());
		// {Cfg.Zookeeper}{"table_iid":"Czhuishusq,Cacfun"}
		try {
			String cfg = "{Cfg.Zookeeper}{\"table_iid\":\"" + 
					new String(client.getData().forPath(configPath)) +"\"}";
			System.err.println("Zk config changed, emit:" + cfg);
			collector.emit(new Values(cfg));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		pool = Executors.newFixedThreadPool(1);
		lock = new ReentrantLock();
		connProduce = lock.newCondition();
		firstStart = true;
		configPath = "/voltdb/partition_by_iid";
		
		this.collector = collector;
		
		this.client = CuratorFrameworkFactory.builder()
				.connectString(configZks)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000,3))
				.build();
		client.start();

		try {
			if (null == client.checkExists().forPath(configPath)){
				System.err.println("ZNode exist not, create it " + configPath);
				client.create().forPath(configPath, "".getBytes());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		final NodeCache nodeCache = new NodeCache(client,configPath,false);
		try {
			nodeCache.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		nodeCache.getListenable().addListener(
				new NodeCacheListener(){
					//@Override
					public void nodeChanged() throws Exception {
						// node changed
						System.err.println("Watched node changed event");
						lock.lock();                        //获取锁 
						connProduce.signalAll();            //唤醒所有等待线程。 
		                lock.unlock();       
						//System.out.println("Node data is changed, new data: " + 
					     //       new String(nodeCache.getCurrentData().getData()));
					}
				},
				pool);
				
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("conf")); //collector.emit(new Values(msg));参数要对应
	}

}
