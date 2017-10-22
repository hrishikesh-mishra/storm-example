package com.hrishikeshmishra.storm.creditcardauthorization.bolts;

import com.hrishikeshmishra.storm.creditcardauthorization.daos.OrderDao;
import com.hrishikeshmishra.storm.creditcardauthorization.exceptions.ServiceException;
import com.hrishikeshmishra.storm.creditcardauthorization.models.Order;
import com.hrishikeshmishra.storm.creditcardauthorization.services.AuthorizationService;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class AuthorizeCreditCard extends BaseRichBolt {

    private AuthorizationService authorizationService;
    private OrderDao orderDao;
    private OutputCollector collector;


    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        orderDao = new OrderDao();
        authorizationService = new AuthorizationService();
        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        Order order = (Order) tuple.getValueByField("order");

        try {

            boolean isAuthorized = authorizationService.authorize(order);
            if (isAuthorized) {
                orderDao.updateStatusReadyToShip(order);
            } else {
                orderDao.updateStatusDenied(order);
            }

            collector.emit(tuple, new Values(order));
            collector.ack(tuple);

        } catch (ServiceException e) {
            collector.fail(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }
}
