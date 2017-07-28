package sea.storm.trident.wordcount;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;

public class WordCountTopology {
	
	private static final Log log = LogFactory.getLog(WordCountTopology.class);
	
	public static StormTopology buildTopology(LocalDRPC drpc){
		
		 FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
			        new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
			        new Values("how many apples can you eat"), new Values("to be or not to be the person"));
			    spout.setCycle(true);

			    TridentTopology topology = new TridentTopology();
			    TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
			        new SentenceSplit(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
			        new Count(), new Fields("count")).parallelismHint(16);

			    if(drpc!=null){
			    topology.newDRPCStream("words", drpc).each(new Fields("args"), new SentenceSplit(), new Fields("word")).groupBy(new Fields(
			        "word")).stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")).each(new Fields("count"),
			        new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));
			    }
			    return topology.build();
	}

	  public static void main(String[] args)
	            throws Exception {
		  Config conf = new Config();
		    conf.setMaxSpoutPending(20);
		    if (args.length == 0) {
		      LocalDRPC drpc = new LocalDRPC();
		      LocalCluster cluster = new LocalCluster();
		      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
		      for (int i = 0; i < 100; i++) {
		    	  log.warn("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
		        Thread.sleep(1000);
		      }
		    }
		    else {
		      conf.setNumWorkers(3);
		      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
		    }
	    }

}
