package com.hrishikeshmishra.storm.barcheckin;

import com.hrishikeshmishra.storm.barcheckin.bolts.GeocodeLookup;
import com.hrishikeshmishra.storm.barcheckin.bolts.HeatMapBuilder;
import com.hrishikeshmishra.storm.barcheckin.bolts.Persistor;
import com.hrishikeshmishra.storm.barcheckin.bolts.TimeIntervalExtractors;
import com.hrishikeshmishra.storm.barcheckin.spouts.Checkins;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class HeatmapTopologyBuilder {

    public StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("checkins",  new Checkins(), 4);

        builder.setBolt("geocode-lookup", new GeocodeLookup(), 8).
                shuffleGrouping("checkins").setNumTasks(64);


        builder.setBolt("time-interval-exteral", new TimeIntervalExtractors(), 4).
                shuffleGrouping("geocode-lookup");


        builder.setBolt("heatmap-builder", new HeatMapBuilder(), 4).fieldsGrouping("time-interval-exteral",
                new Fields("time-interval", "city"));



        builder.setBolt("persistor", new Persistor(), 1).
                setNumTasks(4).
                shuffleGrouping("heatmap-builder");

        return builder.createTopology();
    }
}
