package com.hrishikeshmishra.storm.barcheckin.bolts;

import clojure.lang.IFn;
import com.google.code.geocoder.model.LatLng;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class HeatMapBuilder extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(HeatMapBuilder.class);

    private Map<Long, List<LatLng>> heatMaps;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        heatMaps = Maps.newHashMap();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
        return config;
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (isTickTuple(tuple)) {
            emitHeatmap(basicOutputCollector);
        } else {
            Long timeInterval = tuple.getLongByField("time-interval");
            LatLng geocode = (LatLng) tuple.getValueByField("geocode");

            List<LatLng> checkins = getCheckinsForInterval(timeInterval);
            checkins.add(geocode);

            logger.info("Got in Heat Map : ");
        }

    }

    private void emitHeatmap(BasicOutputCollector basicOutputCollector) {
        Long now = System.currentTimeMillis();
        Long emitUptoTimeInterval = selectTimeInterval(now);
        Set<Long> timeIntervalAvaialble = heatMaps.keySet();

        for (Long timeInterval : timeIntervalAvaialble) {
            if (timeInterval <= emitUptoTimeInterval) {
                List<LatLng> hotzones = heatMaps.remove(timeInterval);
                basicOutputCollector.emit(new Values(timeInterval, hotzones));
            }
        }
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();


        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);

    }

    private List<LatLng> getCheckinsForInterval(Long timeInterval) {
        List<LatLng> hotzones = heatMaps.get(timeInterval);
        if (hotzones == null) {
            hotzones = Lists.newArrayList();
            heatMaps.put(timeInterval, hotzones);
        }

        return hotzones;

    }

    private Long selectTimeInterval(Long time) {
        return time / (15 * 1000);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time-interval", "hotzones"));
    }
}
