package sea.storm.trident.wordcount;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SentenceSplit extends BaseFunction {

	private static final Log log = LogFactory.getLog(SentenceSplit.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		 String sentence = tuple.getString(0);
		// log.debug(sentence);
	       for(String word: sentence.split(" ")) {
	           collector.emit(new Values(word));                
	       }
	}

}
