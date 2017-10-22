package com.hrishikeshmishra.storm.gitcommit.bolts;

import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class EmailCounters extends BaseBasicBolt {
    private Map<String, Integer> counter;

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String email = tuple.getStringByField("email");
        counter.put(email, getCount(email) + 1);

        printCount();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        counter = Maps.newHashMap();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // No output
    }

    private int getCount(String email) {
        Integer count = counter.get(email);
        return count == null ? 0 : count;
    }

    private void printCount() {
        for (String email : counter.keySet()) {
            System.out.printf("========= %s has count of %s \n --------", email, counter.get(email));
        }
    }

}
