package sea.storm.count;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class WordCountBold extends BaseRichBolt {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    HashMap<String,Long> counts=null;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        this.counts=new HashMap<String,Long>();
    }

    @Override
    public void execute(Tuple input) {
        String word=input.getStringByField("word");
        Long count=this.counts.get(word);
        if(count==null){
            count=0l;
        }
        count++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word,count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

}
