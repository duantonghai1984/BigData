package sea.storm.count;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT = "sentence-spout";
    private static final String SPLIT_BOLT     = "split-bolt";
    private static final String COUNT_BOLT     = "count-bolt";
    private static final String REPORT_BOLT    = "report-bolt";
    private static final String TOPOLOGY_NAME  = "word-count-topology";

    public static void main(String[] args)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        SepencesSpout spout = new SepencesSpout();
        SplitBolt splitBolt = new SplitBolt();
        WordCountBold countBolt = new WordCountBold();
        ReportBold reportBolt = new ReportBold();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT, spout);

        builder.setBolt(SPLIT_BOLT, splitBolt).shuffleGrouping(SENTENCE_SPOUT);

        builder.setBolt(COUNT_BOLT, countBolt).fieldsGrouping(SPLIT_BOLT, new Fields("word"));

        builder.setBolt(REPORT_BOLT, reportBolt).globalGrouping(COUNT_BOLT);

        Config config = new Config();
        //config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        } else {
           // config.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            System.out.println("submit local");
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

            Utils.sleep(40000);
            //cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
    }

}
