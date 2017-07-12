package sea.storm.trident.disease;



import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;

public class OutbreakDetectionTopology {
    
    
    public static StormTopology buildTopology(){
        TridentTopology topology=new TridentTopology();
        DiagnosisEventSpout spout=new DiagnosisEventSpout();
        
        Stream inputStream=topology.newStream("event", spout);
        
        inputStream.each(new Fields("event"), new DiseaseFilter())
        
        .each(new Fields("event"), new CityAssignment(), new Fields("city"))
        
        .each(new Fields("event","city"), new HourAssignment(),new Fields("hour","cityDiseaseHour"))
    
        .groupBy(new Fields("cityDiseaseHour"))
        
        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        .newValuesStream()
        
        .each(new Fields("cityDiseaseHour","count"),new OutbreakDector(),new Fields("alert"))
        
        .each(new Fields("alert"),new DispatchAlert(),new Fields());
        
        return null;
    
    }
    

    public static void main(String[] args) {
       

    }

}
