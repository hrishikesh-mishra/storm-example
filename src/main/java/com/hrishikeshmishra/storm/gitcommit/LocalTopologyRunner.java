package com.hrishikeshmishra.storm.gitcommit;

import com.hrishikeshmishra.storm.gitcommit.bolts.EmailCounters;
import com.hrishikeshmishra.storm.gitcommit.bolts.EmailExtractor;
import com.hrishikeshmishra.storm.gitcommit.spouts.CommitFeedListener;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class LocalTopologyRunner {

    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());


        builder.setBolt("email-extractor", new EmailExtractor()).
                shuffleGrouping("commit-feed-listener");


        builder.setBolt("email-counter", new EmailCounters()).
                fieldsGrouping("email-extractor", new Fields("email"));


        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();


        LocalCluster cluster = new LocalCluster();


        cluster.submitTopology("github-commit-counter-topology", config, topology);

        Utils.sleep(TEN_MINUTES);


        cluster.killTopology("github-commit-counter-topology");
        cluster.shutdown();



    }

}
