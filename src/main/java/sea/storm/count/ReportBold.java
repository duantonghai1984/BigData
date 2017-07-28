package sea.storm.count;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;



public class ReportBold extends BaseRichBolt {

 
    private static final long serialVersionUID = 1L;
    HashMap<String,Long> counts=null;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
       
        this.counts=new HashMap<String,Long>();
    }

    @Override
    public void execute(Tuple input) {
        String word=input.getStringByField("word");
        
        Long count=input.getLongByField("count");
       // System.out.println(word+":"+count);
        this.counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

    @Override
    public void cleanup() {
        System.out.println("clean up");
     for(String key: this.counts.keySet()){
         System.out.println(key+":"+this.counts.get(key));
     }
     System.out.println("clean up over");
    }
    
    

}
