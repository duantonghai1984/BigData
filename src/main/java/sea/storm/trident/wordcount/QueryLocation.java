package sea.storm.trident.wordcount;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class QueryLocation extends BaseQueryFunction<LocationDB, String> {

	@Override
	public List<String> batchRetrieve(LocationDB state, List<TridentTuple> args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(TridentTuple tuple, String result, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

}
