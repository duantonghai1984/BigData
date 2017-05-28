package sea.storm.trident.disease;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

public class DefaultCoordinator implements BatchCoordinator<Long> {

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Long initializeTransaction(long arg0, Long arg1, Long arg2) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isReady(long arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void success(long arg0) {
        // TODO Auto-generated method stub
        
    }

}
