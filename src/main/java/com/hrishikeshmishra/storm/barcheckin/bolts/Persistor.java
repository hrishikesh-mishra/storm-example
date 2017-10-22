package com.hrishikeshmishra.storm.barcheckin.bolts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.code.geocoder.model.LatLng;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Persistor extends BaseBasicBolt {


    private Logger logger = LoggerFactory.getLogger(Persistor.class);

    private Jedis jedis;
    private ObjectMapper objectMapper;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        jedis = new Jedis("localhost");
        objectMapper = new ObjectMapper();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        Long timeInterval = tuple.getLongByField("time-interval");
        List<LatLng> hz = (List<LatLng>) tuple.getValueByField("hotzones");

        List<String> hotzones = asListOfZones(hz);

        try {
            String key = "checkins-" + timeInterval;
            String value = objectMapper.writeValueAsString(hotzones);

            logger.info("Persistin : {}", key );

            jedis.set(key, value);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private List<String> asListOfZones(List<LatLng> hz) {
        List<String> list = new ArrayList<String>(hz.size());

        for (LatLng geoCor : hz) {
            list.add(geoCor.toUrlValue());
        }

        return list;

    }

    @Override
    public void cleanup() {

        if (jedis.isConnected()) {
            jedis.quit();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //don't do anything
    }
}
