package com.hrishikeshmishra.storm.barcheckin.bolts;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.*;
import com.hrishikeshmishra.storm.barcheckin.spouts.Checkins;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class GeocodeLookup extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(GeocodeLookup.class);

    private Geocoder geocoder;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        geocoder = new Geocoder();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String address = tuple.getStringByField("address");
        Long time = tuple.getLongByField("time");


        GeocoderRequest request = new GeocoderRequestBuilder().setAddress(address)
                .setLanguage("en")
                .getGeocoderRequest();

        try {

            GeocodeResponse response = geocoder.geocode(request);
            GeocoderStatus status = response.getStatus();

            if (GeocoderStatus.OK.equals(status)) {
                GeocoderResult firstResult = response.getResults().get(0);
                LatLng latLng = firstResult.getGeometry().getLocation();
                String city = extractCity(firstResult);

                logger.info("{} time --- {} LatLng , city : {} ", time, latLng, city);

                basicOutputCollector.emit(new Values(time, latLng, city));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private String extractCity(GeocoderResult result) {
        for (GeocoderAddressComponent component : result.getAddressComponents()) {

            if (component.getTypes().contains("locality")) {
                return component.getLongName();
            }
        }

        return "";
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time", "geocode", "city"));
    }
}
