package sea.storm.trident.disease;

import java.math.BigInteger;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ICommitterTransactionalSpout.Emitter;
import backtype.storm.transactional.TransactionAttempt;
import storm.trident.operation.TridentCollector;

public class DiagnosisEventEmitter implements Emitter{

	@Override
	public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, BatchOutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupBefore(BigInteger txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(TransactionAttempt attempt) {
		// TODO Auto-generated method stub
		
	}

     
   

}
