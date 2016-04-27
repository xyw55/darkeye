package com.xyw55.test.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.xyw55.Risk;
import com.xyw55.dao.JedisTemplate;
import com.xyw55.dao.TimeCounterHelper;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xiayiwei on 16/4/24.
 */
public class AlertBolt extends BaseRichBolt {
    private static TimeCounterHelper timeCounterHelper = new TimeCounterHelper();
    private static JedisTemplate jedisTemplate = new JedisTemplate(new JedisPool());
    private OutputCollector _collector = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
//        System.out.println("alert||||" + tuple);
        ArrayList<Risk> risks = new ArrayList<Risk>();
        risks = (ArrayList<Risk>) tuple.getValueByField("threats");
        String pcap_id = tuple.getStringByField("pcap_id");
        int score = 0;
        for (Risk risk: risks) {
            if (risk.getType().equals("black_domain")) {
                risk.setScore(5);
            } else if (risk.getType().equals("webshell")) {
                risk.setScore(10);
            }
        }

        updateRiskCounters(pcap_id, risks);
        _collector.emit("alerts", new Values(pcap_id, risks));
        _collector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("alerts", new Fields("pcap_id", "risks"));

    }


    private void updateRiskCounters(String pcap_id, ArrayList<Risk> risks){
        /**
         * 根据pcap_id查询header,得到ip,入redis
         */
        String key  = "darkeye:header:" + pcap_id;

        String header_json_str = jedisTemplate.get(key);
        if (header_json_str == null) {
            return;
        }
        JSONObject message = (JSONObject) JSONValue.parse(header_json_str);
        JSONObject ipv4_header = (JSONObject) message.get("ipv4_header");
        String src_ip = (String) ipv4_header.get("ip_src_addr");
        String dst_ip = (String) ipv4_header.get("ip_dst_addr");
        for (Risk risk : risks) {

//            timeCounterHelper.incrTimeCounterSnaps("risk_score", risk.getType(), null, (int) risk.getScore());
            timeCounterHelper.incrTimeCounterSnaps("risk_score", src_ip, null, (int) risk.getScore());
            timeCounterHelper.incrTimeCounterSnaps("risk_score", dst_ip, null, (int) risk.getScore());
            timeCounterHelper.incrTimeCounterSnaps(src_ip, pcap_id, null, (int) risk.getScore());
            timeCounterHelper.incrTimeCounterSnaps(dst_ip, pcap_id, null, (int) risk.getScore());
//            timeCounterHelper.incrTimeCounterSnaps("risk_count", risk.getType());
        }
    }


}
