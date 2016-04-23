package com.xyw55.topology.runner;

import com.xyw55.parsing.PcapParserBolt;
import com.xyw55.test.spouts.GenericInternalTestSpout;

/**
 * Created by xiayiwei on 16/3/26.
 */
public class PcapRunner extends TopologyRunner {

    static String test_file_path = "/Users/xiayiwei/Documents/finalPaper/code/darkeye/DarkEye-Topology/src/main/resources/SampleInput/pcap7.pcap";
//    static String test_file_path = "SampleInput/PCAPExampleOutput";

    @Override
    protected void initializeTestingSpout(String component_name) {
        try {

            GenericInternalTestSpout testSpout = new GenericInternalTestSpout()
                    .withFilename(test_file_path)
                    .withRepeating(configuration.getBoolean("spout.test.parallelism.repeat"));

            builder.setSpout(component_name, testSpout,
                    configuration.getInt("spout.test.parallelism.hint"))
                    .setNumTasks(configuration.getInt("spout.test.num.tasks"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

    @Override
    boolean initializeParsingBolt(String component_name) {
        try {

            String messageUpstreamComponent = "TestSpout";

            System.out.println("[OpenSOC] ------" +  component_name + " is initializing from " + messageUpstreamComponent);

            PcapParserBolt pcapParser = new PcapParserBolt().withTsPrecision(configuration.getString("bolt.parser.ts.precision"));

            builder.setBolt(component_name, pcapParser,
                    configuration.getInt("bolt.parser.parallelism.hint"))
                    .setNumTasks(configuration.getInt("bolt.parser.num.tasks"))
                    .shuffleGrouping(messageUpstreamComponent);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return true;
    }
}
