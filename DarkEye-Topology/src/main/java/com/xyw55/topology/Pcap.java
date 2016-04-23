package com.xyw55.topology;

import com.xyw55.topology.runner.PcapRunner;
import com.xyw55.topology.runner.TopologyRunner;


/**
 * Created by xiayiwei on 16/3/26.
 */
public class Pcap {

    public static void main(String[] args) throws Exception {
        TopologyRunner runner = new PcapRunner();
        runner.initTopology(args);
    }
}
