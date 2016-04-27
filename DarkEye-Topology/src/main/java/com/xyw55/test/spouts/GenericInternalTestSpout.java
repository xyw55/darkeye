package com.xyw55.test.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.xyw55.pacp.PacketInfo;
import com.xyw55.test.filereaders.FileReader;
import org.apache.commons.io.FileUtils;
import org.krakenapps.pcap.util.ChainBuffer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by xiayiwei on 16/3/26.
 */
public class GenericInternalTestSpout extends BaseRichSpout{
    private static final long serialVersionUID = -2379344923143372543L;

    List<String> jsons;

    private String _filename;
    private int _delay = 100;
    private boolean _repeating = true;
    private byte[] pcapBytes;

    private SpoutOutputCollector _collector;
    private FileReader Reader;
    private int cnt = 0;

    public GenericInternalTestSpout withFilename(String filename)
    {
        _filename = filename;
        return this;
    }
    public GenericInternalTestSpout withMilisecondDelay(int delay)
    {
        _delay = delay;
        return this;
    }

    public GenericInternalTestSpout withRepeating(boolean repeating)
    {
        _repeating = repeating;
        return this;
    }


    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        _collector = collector;
        try {
//            Reader =  new FileReader();
//            jsons = Reader.readFromFile(_filename);
            File fin = new File(_filename);
            pcapBytes = FileUtils.readFileToByteArray(fin);

        } catch (IOException e)
        {
            System.out.println("Could not read sample JSONs");
            e.printStackTrace();
        }

    }

    public void nextTuple() {
        Utils.sleep(_delay);

//        if(cnt < jsons.size())
//        {
//            _collector.emit(new Values(jsons.get(cnt).getBytes()));
//        }
//        cnt ++;
//
//        if(_repeating && cnt == jsons.size() -1 )
//            cnt = 0;

        _collector.emit(new Values(pcapBytes));
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }


}
