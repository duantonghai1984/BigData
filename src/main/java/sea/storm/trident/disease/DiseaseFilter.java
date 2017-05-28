package sea.storm.trident.disease;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class DiseaseFilter extends BaseFilter{

    @Override
    public boolean isKeep(TridentTuple arg0) {
        // TODO Auto-generated method stub
        return false;
    }

}
