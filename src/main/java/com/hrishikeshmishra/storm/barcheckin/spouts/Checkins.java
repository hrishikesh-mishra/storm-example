package com.hrishikeshmishra.storm.barcheckin.spouts;

import com.hrishikeshmishra.storm.barcheckin.bolts.Persistor;
import org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.IOUtil;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class Checkins extends BaseRichSpout {

    private Logger logger = LoggerFactory.getLogger(Checkins.class);

    private List<String> checkins;
    private int nextEmitIndex;
    private SpoutOutputCollector spoutOutputCollector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.spoutOutputCollector = spoutOutputCollector;
        this.nextEmitIndex = 0;

        try {
            checkins = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("bar-checkin-changelog.txt"),
                    Charset.defaultCharset().name());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void nextTuple() {
        String checkin = checkins.get(nextEmitIndex);
        String[] parts = checkin.split(",");
        Long time = Long.valueOf(parts[0]);
        String address = parts[1];

        logger.info("Got new Checkin : {} ", checkin);


        //Utils.sleep(1000);

        spoutOutputCollector.emit(new Values(time, address));

        nextEmitIndex = (nextEmitIndex + 1) % checkins.size();

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time", "address"));
    }
}
