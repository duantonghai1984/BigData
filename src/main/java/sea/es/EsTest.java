package sea.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
/*
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


public class EsTest {
    
    private String host="127.0.0.1";
    private int port=9300;
    
    private TransportClient client=null;
    
    private TransportClient getClient() throws UnknownHostException{
        if(client==null){
        Settings settings = Settings.builder()
                .put("cluster.name", "duantSearch").build();
         client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        }
        return client;
    }
    
    
    private IndexRequestBuilder addInex(String index,String type,String id, Map<String, Object> data) throws Exception{
        IndexRequestBuilder response = getClient().prepareIndex(index, type,id)
                .setSource(data);
        
        return response;
    }
    
    
    private void updateInex(String index,String type,String id, Map<String, Object> data) throws Exception{
        UpdateRequest updateRequest = new UpdateRequest(index, type,id)
                .doc(data);
        getClient().update(updateRequest).get();
        return ;
    }
    
    
    private  Map<String, Object> getAddedData(){
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user","kimchy");
        json.put("postDate",new Date());
        json.put("message","trying out Elasticsearch");
        
        return json;
    }
    
    
    private  Map<String, Object> getUpData(){
        Map<String, Object> upMap = new HashMap<String, Object>();
        upMap.put("user","duant");
        
        return upMap;
    }

    public static void main(String[] args) throws Exception {
        EsTest test=new EsTest();
        String index="twitter";
        String type="tweet";
        String id="1";
        
        test.addInex(index, type, id, test.getAddedData());
        
        test.updateInex(index, type, id, test.getUpData());
           
     
    }

}
*/