package sea.storm.count;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;



public class SepencesSpout extends BaseRichSpout{

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private String[] sentences={
            "my dog is dog",
            "chaine is dog",
            "li is dog",
            "wang is dog"
    };
    private int index=0;
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void nextTuple() {
        if( index>=sentences.length){
            //index=0;
            return;
        }
        this.collector.emit(new Values(sentences[index]));
        ++index;
       
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}
