package com.hrishikeshmishra.storm.barcheckin.bolts;

import com.google.code.geocoder.model.LatLng;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TimeIntervalExtractors extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        Long time = tuple.getLongByField("time");
        String city = tuple.getStringByField("city");

        LatLng geocode = (LatLng) tuple.getValueByField("geocode");

        Long timeInterval = time / (15 * 1000);
        basicOutputCollector.emit(new Values(timeInterval, geocode, city));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time-interval", "geocode", "city"));
    }
}
