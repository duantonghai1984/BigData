package sea.storm.count;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;



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
