package com.xyw55.test.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by xiayiwei on 16/4/22.
 */
public class ThreatBolt extends BaseRichBolt {

    private OutputCollector _collector = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        try {
            System.out.println("threadBolt=== " + tuple);
            _collector.ack(tuple);
        } catch (Exception e) {
            _collector.fail(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
