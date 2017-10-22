package com.hrishikeshmishra.storm.barcheckin.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Persistor extends BaseBasicBolt {


    private Logger logger = LoggerFactory.getLogger(Persistor.class);

//    private Jedis jedis;
//    private ObjectMapp

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
