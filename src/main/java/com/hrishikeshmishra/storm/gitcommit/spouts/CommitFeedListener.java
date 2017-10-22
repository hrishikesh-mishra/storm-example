package com.hrishikeshmishra.storm.gitcommit.spouts;


import org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class CommitFeedListener extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    private List<String> commits;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;

        try {
            commits = IOUtils.readLines(
                    ClassLoader.getSystemResourceAsStream("commit-changelog.txt"),
                    Charset.defaultCharset().name());

            //System.out.println("\n\n\n\n\n\n -------------------- " + commits + "\n\n\n\n\n");
        } catch (IOException e) {
            //do do anything for now.
        }


    }

    public void nextTuple() {
        for (String commit : commits) {
            outputCollector.emit(new Values(commit));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("commit"));
    }
}
