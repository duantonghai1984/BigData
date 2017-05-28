package sea.storm.trident.disease;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;

public class DiagnosisEventEmitter implements Emitter<Long>{

     
    
    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void emitBatch(TransactionAttempt arg0, Long arg1, TridentCollector arg2) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void success(TransactionAttempt arg0) {
        // TODO Auto-generated method stub
        
    }

}
