package com.hrishikeshmishra.storm.creditcardauthorization.bolts;

import com.hrishikeshmishra.storm.creditcardauthorization.models.Order;
import com.hrishikeshmishra.storm.creditcardauthorization.services.NotificationService;
import com.sun.tools.corba.se.idl.constExpr.Or;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class ProcessedOrderNotification extends BaseBasicBolt {

    private NotificationService notificationService;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        notificationService = new NotificationService();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        Order order = (Order) tuple.getValueByField("order");
        notificationService.notifyOrderHasBeenProcessed(order);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //Nothing to emit
    }
}
