package sea.storm.trident.disease;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.ITridentSpout.BatchCoordinator;

public class DiagnosisEventSpout implements ITridentSpout<Long>{

    private static final long serialVersionUID = 1L;

	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<Long> getCoordinator(String txStateId, Map conf,
			TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<Long> getEmitter(String txStateId, Map conf,
			TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

   

}
