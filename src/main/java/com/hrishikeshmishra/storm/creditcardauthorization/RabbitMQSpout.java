package com.hrishikeshmishra.storm.creditcardauthorization;

import com.google.gson.Gson;
import com.hrishikeshmishra.storm.creditcardauthorization.models.Order;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQSpout extends BaseRichSpout {

    private Connection connection;
    private Channel channel;
    private QueueingConsumer consumer;
    private SpoutOutputCollector outputCollector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        outputCollector = spoutOutputCollector;
        try {
            connection = new ConnectionFactory().newConnection();
            channel = connection.createChannel();

            channel.basicQos(25);
            consumer = new QueueingConsumer(channel);
            channel.basicConsume("orders", false, consumer);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


    }

    public void nextTuple() {

        try {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(1L);


            if (delivery == null) {
                return;
            }


            Long messageId = delivery.getEnvelope().getDeliveryTag();
            byte[] messageBytes = delivery.getBody();

            String messageAsString = new String(messageBytes, Charset.forName("UTF-8"));

            Order order = new Gson().fromJson(messageAsString, Order.class);

            outputCollector.emit(new Values(order), messageId);


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void ack(Object msgId) {
        try {
            channel.basicAck((Long) msgId, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            channel.basicReject((Long) msgId, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

        try {
            channel.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }
}
