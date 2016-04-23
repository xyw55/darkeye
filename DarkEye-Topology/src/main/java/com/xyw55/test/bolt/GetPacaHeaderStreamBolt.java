package com.xyw55.test.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Map;

/**
 * Created by xiayiwei on 16/4/10.
 */
public class GetPacaHeaderStreamBolt extends BaseRichBolt {
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        try {
            String pcap_id = tuple.getStringByField("pcap_id");
            String header_json_str = tuple.getStringByField("header_json");
            Object obj = JSONValue.parse(header_json_str);
            JSONObject header_json = (JSONObject)obj;
            System.out.println("==========header========== " + pcap_id + "||||||" + header_json);
            if (header_json == null || header_json.isEmpty())
                throw new Exception(
                        "Could not parse message from binary stream");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("=======header=error=======" + tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
