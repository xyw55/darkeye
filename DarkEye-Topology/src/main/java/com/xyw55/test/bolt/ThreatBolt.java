package com.xyw55.test.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.xyw55.Risk;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiayiwei on 16/4/22.
 */
public class ThreatBolt extends BaseRichBolt {

    private OutputCollector _collector = null;
    private ArrayList<String> _blacklist_table = new ArrayList<String>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        _blacklist_table.add("www.baiu.com");
        _blacklist_table.add("hm.baidu.com");

    }

    public void execute(Tuple tuple) {
        try {
//            System.out.println("threadBolt=== " + tuple);
            ArrayList<Risk> risks = new ArrayList<Risk>();
            JSONObject httpMessage = (JSONObject) tuple.getValueByField("httpMessage");
            System.out.println(httpMessage.toString());
            String pcap_id = (String) httpMessage.get("pcap_id");
            if (httpMessage.containsKey("Host")) {
                String host = (String) httpMessage.get("Host");
                if (_blacklist_table.contains(host)) {
                    System.out.println("black host--" + host);
                    Risk risk = new Risk();
                    risk.setPcap_id((String) httpMessage.get("pcap_id"));
                    risk.setType("black_domain");
                    risk.setContent(host);
                    risks.add(risk);
                }
            }
            if (httpMessage.containsKey("body")) {
                String body = (String) httpMessage.get("body");
                if (body.startsWith("GIF89a")) {
                    System.out.println("webshell--" + body);
                    Risk risk = new Risk();
                    risk.setPcap_id((String) httpMessage.get("pcap_id"));
                    risk.setType("webshell");
                    risk.setContent(body);
                    risks.add(risk);
                }
            }
            _collector.emit("threats", new Values(pcap_id, risks));
            _collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
            _collector.fail(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("threats", new Fields("pcap_id", "threats"));

    }
}
