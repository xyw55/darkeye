package com.xyw55.indexing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.xyw55.helper.topology.ErrorGenerator;
import com.xyw55.index.interfaces.IndexAdapter;
import com.xyw55.json.serialization.JSONEncoderHelper;
import org.apache.commons.configuration.Configuration;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by xiayiwei on 16/4/23.
 */
public class BodyIndexingBolt extends AbstractIndexingBolt {

    private JSONObject metricConfiguration;
    private String _indexDateFormat;

    private Set<Tuple> tuple_queue = new HashSet<Tuple>();

    /**
     *
     * @param IndexIP
     *            ip of ElasticSearch/Solr/etc...
     * @return instance of bolt
     */
    public BodyIndexingBolt withIndexIP(String IndexIP) {
        _IndexIP = IndexIP;
        return this;
    }

    /**
     *
     * @param IndexPort
     *            port of ElasticSearch/Solr/etc...
     * @return instance of bolt
     */

    public BodyIndexingBolt withIndexPort(int IndexPort) {
        _IndexPort = IndexPort;
        return this;
    }

    /**
     *
     * @param IndexName
     *            name of the index in ElasticSearch/Solr/etc...
     * @return instance of bolt
     */
    public BodyIndexingBolt withIndexName(String IndexName) {
        _IndexName = IndexName;
        return this;
    }

    /**
     *
     * @param ClusterName
     *            name of cluster to index into in ElasticSearch/Solr/etc...
     * @return instance of bolt
     */
    public BodyIndexingBolt withClusterName(String ClusterName) {
        _ClusterName = ClusterName;
        return this;
    }

    /**
     *
     * @param DocumentName
     *            name of document to be indexed in ElasticSearch/Solr/etc...
     * @return
     */

    public BodyIndexingBolt withDocumentName(String DocumentName) {
        _DocumentName = DocumentName;
        return this;
    }

    /**
     *
     * @param BulkIndexNumber
     *            number of documents to bulk index together
     * @return instance of bolt
     */
    public BodyIndexingBolt withBulk(int BulkIndexNumber) {
        _BulkIndexNumber = BulkIndexNumber;
        return this;
    }

    /**
     *
     * @param adapter
     *            adapter that handles indexing of JSON strings
     * @return instance of bolt
     */
    public BodyIndexingBolt withIndexAdapter(IndexAdapter adapter) {
        _adapter = adapter;

        return this;
    }

    /**
     *
     //	 * @param dateFormat
     *           timestamp to append to index names
     * @return instance of bolt
     */
    public BodyIndexingBolt withIndexTimestamp(String indexTimestamp) {
        _indexDateFormat = indexTimestamp;

        return this;
    }
    /**
     *
     * @param config
     *            - configuration for pushing metrics into graphite
     * @return instance of bolt
     */
    public BodyIndexingBolt withMetricConfiguration(Configuration config) {
        this.metricConfiguration = JSONEncoderHelper.getJSON(config
                .subset("com.opensoc.metrics"));
        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    void doPrepare(Map conf, TopologyContext topologyContext,
                   OutputCollector collector) throws IOException {

        try {

            _adapter.initializeConnection(_IndexIP, _IndexPort,
                    _ClusterName, _IndexName, _DocumentName, _BulkIndexNumber, _indexDateFormat);

//			_reporter = new MetricReporter();
//			_reporter.initialize(metricConfiguration,
//					BodyIndexingBolt.class);
            this.registerCounters();
        } catch (Exception e) {

            e.printStackTrace();

            JSONObject error = ErrorGenerator.generateErrorMessage(new String("bulk index problem"), e);
            _collector.emit("error", new Values(error));
        }

    }

    public void execute(Tuple tuple) {

        String key = null;
        JSONObject message = new JSONObject();

        try {

            String pcap_id = tuple.getStringByField("pcap_id");
            message.put("pcap_id", pcap_id);
            Long timestamp = tuple.getLongByField("timestamp");
            message.put("timestamp", timestamp);
            String httpBody = (String) tuple.getValueByField("body");
            Pattern pattern = Pattern.compile("\r\n");
            String[] strs = pattern.split(httpBody);
            Map map = new HashMap();
            for (int i = 0; i < strs.length; i++) {
//                System.out.println(strs[i]);
                if (i == 0) {
                    /**
                     * request:
                     * GET /hm.gif?cc=0&ck=1 HTTP/1.1
                     * response:
                     * HTTP/1.1 200 OK
                     */
                    String firstLine = strs[0];
                    int firstSpace = firstLine.indexOf(" ");
                    int sencondSpace = firstLine.indexOf(" ", firstSpace + 1);
                    if (firstSpace == -1 || sencondSpace == -1) {
                        continue;
                    }
                    String firstStr = firstLine.substring(0, firstSpace);
                    String secondStr = firstLine.substring(firstSpace + 1, sencondSpace);
                    String thridStr = firstLine.substring(sencondSpace + 1);
                    if (!firstStr.startsWith("HTTP")) {
                        message.put("method", firstStr);
                        message.put("uri", secondStr);
                        message.put("version", thridStr);
                    } else {
                        message.put("version", firstStr);
                        message.put("statusCode", secondStr);
                        message.put("statusInfo", thridStr);
                    }
                } else if (strs[i].equals("")) {
                    if (strs.length == i + 1) {
                        message.put("body", strs[i + 1]);
                    } else {
                        message.put("body", "");
                    }
                    break;
                } else {
                    String[] commonLine = strs[i].split(":");
                    if (commonLine.length == 2) {
                        message.put(commonLine[0], commonLine[1]);
                    }
                }
            }

//            System.out.println("-----------index------------" + tuple + "||||||" + pcap_id + "||||||" + message);
            if (message == null || message.isEmpty())
                throw new Exception(
                        "Could not parse message from binary stream");

            int result_code = _adapter.bulkIndex(message);

            if (result_code == 0) {
                tuple_queue.add(tuple);
            } else if (result_code == 1) {
                tuple_queue.add(tuple);

                Iterator<Tuple> iterator = tuple_queue.iterator();
                while(iterator.hasNext())
                {
                    Tuple setElement = iterator.next();
                    _collector.ack(setElement);
//					ackCounter.inc();
                }
                tuple_queue.clear();
            } else if (result_code == 2) {
                throw new Exception("Failed to index elements with client");
            }
            _collector.emit("httpMessage", new Values(message));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
            Iterator<Tuple> iterator = tuple_queue.iterator();
            while(iterator.hasNext())
            {
                Tuple setElement = iterator.next();
                _collector.fail(setElement);
//				failCounter.inc();

                JSONObject error = ErrorGenerator.generateErrorMessage(new String("bulk index problem"), e);
                _collector.emit("error", new Values(error));
            }
            tuple_queue.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declearer) {
        declearer.declareStream("error", new Fields("Index"));
        declearer.declareStream("httpMessage", new Fields("httpMessage"));
    }

}
