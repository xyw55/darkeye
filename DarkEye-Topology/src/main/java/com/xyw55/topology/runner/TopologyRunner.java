package com.xyw55.topology.runner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.xyw55.helper.topology.SettingsLoader;
import com.xyw55.index.interfaces.IndexAdapter;
import com.xyw55.indexing.BodyIndexingBolt;
import com.xyw55.indexing.HeaderIndexingBolt;
import com.xyw55.indexing.TelemetryIndexingBolt;
import com.xyw55.test.bolt.GetPacaBodyStreamBolt;
import com.xyw55.test.bolt.GetPacaHeaderStreamBolt;
import com.xyw55.test.bolt.ThreatBolt;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.simple.JSONObject;

import java.util.Map;

/**
 * Created by xiayiwei on 16/3/26.
 */
public abstract class TopologyRunner {
    protected Configuration configuration;
    protected Config config;
    protected TopologyBuilder builder;
    protected boolean local_mode = true;
    protected boolean debug = true;
    protected String config_path = null;
    protected String default_config_path = "/Users/xiayiwei/Documents/finalPaper/code/darkeye/DarkEye-Topology/src/main/resources/DarkEye_Configs";
    private static final String topology_name = "word-count-topology";
    protected boolean success = false;

    public void initTopology(String args[]) throws Exception {

        config_path = default_config_path;
        String topology_conf_path = config_path + "/topology/topology.conf";

        configuration = new PropertiesConfiguration(topology_conf_path);
        builder = new TopologyBuilder();
        config = new Config();
        config.registerSerialization(JSONObject.class, MapSerializer.class);
        config.setDebug(debug);

        if (true) {
            String component_name = configuration.getString("spout.test.name",
                    "DeauftTopologySpout");

            initializeTestingSpout(component_name);
        }
        if (configuration.getBoolean("bolt.parser.enabled", true)) {
            String temp_component_name = configuration.getString("bolt.parser.name",
                    "DefaultTopologyParserBot");

            initializeParsingBolt(temp_component_name);

        }
        if (configuration.getBoolean("bolt.indexing.enabled", true)) {
            String component_name = configuration.getString("bolt.indexing.name",
                    "DefaultIndexingBolt");

            success = initializeIndexingBolt(component_name);
//            errorComponents.add(component_name);
//            terminalComponents.add(component_name);

            System.out.println("[OpenSOC] ------Component " + component_name
                    + " initialized with the following settings:");

            SettingsLoader.printConfigOptions((PropertiesConfiguration) configuration,
                    "bolt.indexing");
        }

//        if (true) {
//
//            String component_name = configuration.getString("bolt.pacaHeaderStreamBoltt.name",
//                    "DefaultPacaHeaderStreamBolt");
//
//            success = initializePacaHeaderStreamBolt(component_name);
//        }
//
//        if (true) {
//
//            String component_name = configuration.getString("bolt.pacaBodyStreamBoltt.name",
//                    "DefaultPacaBodyStreamBolt");
//
//            success = initializePacaBodyStreamBolt(component_name);
//        }


        if (true) {
            String component_name = configuration.getString("bolt.indexing.header.name",
                    "DefaultHeaderIndexingBolt");

            success = initializeHeaderIndexingBolt(component_name);
//            errorComponents.add(component_name);
//            terminalComponents.add(component_name);

            System.out.println("[OpenSOC] ------Component " + component_name
                    + " initialized with the following settings:");

            SettingsLoader.printConfigOptions((PropertiesConfiguration) configuration,
                    "bolt.indexing");
        }

        if (true) {
            String component_name = configuration.getString("bolt.indexing.body.name",
                    "DefaultBodyIndexingBolt");

            success = initializeBodyIndexingBolt(component_name);
//            errorComponents.add(component_name);
//            terminalComponents.add(component_name);

            System.out.println("[OpenSOC] ------Component " + component_name
                    + " initialized with the following settings:");

            SettingsLoader.printConfigOptions((PropertiesConfiguration) configuration,
                    "bolt.indexing");
        }

        if (true) {
            String component_name = configuration.getString("bolt.threat.http.name",
                    "DefaultThreatHttpBolt");
            success = initializeThreatHttpBolt(component_name);
            System.out.println("[OpenSOC] ------Component " + component_name
                    + " initialized with the following settings:");

            SettingsLoader.printConfigOptions((PropertiesConfiguration) configuration,
                    "bolt.threat");
        }

        if (local_mode) {
            config.setNumWorkers(configuration.getInt("num.workers"));
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topology_name, config,
                    builder.createTopology());
        } else {

            config.setNumWorkers(configuration.getInt("num.workers"));
            config.setNumAckers(configuration.getInt("num.ackers"));
            StormSubmitter.submitTopology(topology_name, config,
                    builder.createTopology());
        }
    }

    private boolean initializeThreatHttpBolt(String name) {
        String messageUpstreamComponent = "BodyIndexBolt";
        try {
            System.out.println("[OpenSOC] ------" + name
                    + " is initializing from " + messageUpstreamComponent);
            ThreatBolt threatBolt = new ThreatBolt();
            builder.setBolt(name, threatBolt)
                    .shuffleGrouping(messageUpstreamComponent, "httpMessage")
                    .setNumTasks(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return true;
    }

    private boolean initializeBodyIndexingBolt(String name) {
        String messageUpstreamComponent = "ParserBolt";
        try {
            System.out.println("[OpenSOC] ------" + name
                    + " is initializing from " + messageUpstreamComponent);
            /**
             * adapter class
             * com.xyw55.indexing.adapters.ESTimedRotatingAdapter
             */
            Class loaded_class = Class.forName(configuration.getString("bolt.indexing.body.adapter"));
            IndexAdapter adapter = (IndexAdapter) loaded_class.newInstance();

            Map<String, String> settings = SettingsLoader.getConfigOptions((PropertiesConfiguration)configuration, "optional.settings.bolt.index.search.");

            if(settings != null && settings.size() > 0)
            {
                adapter.setOptionalSettings(settings);
                System.out.println("[OpenSOC] Index Bolt picket up optional settings:");
                SettingsLoader.printOptionalSettings(settings);
            }

            // dateFormat defaults to hourly if not specified
            String dateFormat = "yyyy.MM.dd.hh";
            if (configuration.containsKey("bolt.indexing.body.timestamp")) {
                dateFormat = configuration.getString("bolt.indexing.body.timestamp");
            }
            BodyIndexingBolt indexing_bolt = new BodyIndexingBolt()
                    .withIndexIP(configuration.getString("es.ip"))
                    .withIndexPort(configuration.getInt("es.port"))
                    .withClusterName(configuration.getString("es.clustername"))
                    .withIndexName(configuration.getString("bolt.indexing.body.indexname"))
                    .withIndexTimestamp(dateFormat)
                    .withDocumentName(
                            configuration.getString("bolt.indexing.body.documentname"))
                    .withBulk(configuration.getInt("bolt.indexing.body.bulk"))
                    .withIndexAdapter(adapter)
                    .withMetricConfiguration(configuration);

            builder.setBolt(name, indexing_bolt,
                    configuration.getInt("bolt.indexing.parallelism.hint"))
                    .shuffleGrouping(messageUpstreamComponent, "pcap_body_stream")
                    .setNumTasks(configuration.getInt("bolt.indexing.num.tasks"));

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return true;
    }

    private boolean initializeHeaderIndexingBolt(String name) {
        String messageUpstreamComponent = "ParserBolt";
        try {
            System.out.println("[OpenSOC] ------" + name
                    + " is initializing from " + messageUpstreamComponent);
            /**
             * adapter class
             * com.xyw55.indexing.adapters.ESTimedRotatingAdapter
             */
            Class loaded_class = Class.forName(configuration.getString("bolt.indexing.header.adapter"));
            IndexAdapter adapter = (IndexAdapter) loaded_class.newInstance();

            Map<String, String> settings = SettingsLoader.getConfigOptions((PropertiesConfiguration)configuration, "optional.settings.bolt.index.search.");

            if(settings != null && settings.size() > 0)
            {
                adapter.setOptionalSettings(settings);
                System.out.println("[OpenSOC] Index Bolt picket up optional settings:");
                SettingsLoader.printOptionalSettings(settings);
            }

            // dateFormat defaults to hourly if not specified
            String dateFormat = "yyyy.MM.dd.hh";
            if (configuration.containsKey("bolt.indexing.header.timestamp")) {
                dateFormat = configuration.getString("bolt.indexing.header.timestamp");
            }
            HeaderIndexingBolt indexing_bolt = new HeaderIndexingBolt()
                    .withIndexIP(configuration.getString("es.ip"))
                    .withIndexPort(configuration.getInt("es.port"))
                    .withClusterName(configuration.getString("es.clustername"))
                    .withIndexName(configuration.getString("bolt.indexing.header.indexname"))
                    .withIndexTimestamp(dateFormat)
                    .withDocumentName(
                            configuration.getString("bolt.indexing.header.documentname"))
                    .withBulk(configuration.getInt("bolt.indexing.header.bulk"))
                    .withIndexAdapter(adapter)
                    .withMetricConfiguration(configuration);

            builder.setBolt(name, indexing_bolt,
                    configuration.getInt("bolt.indexing.parallelism.hint"))
                    .shuffleGrouping(messageUpstreamComponent, "pcap_header_stream")
                    .setNumTasks(configuration.getInt("bolt.indexing.num.tasks"));

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return true;
    }

    private boolean initializePacaBodyStreamBolt(String name) {
        String messageUpstreamComponent = "ParserBolt";
        System.out.println("[OpenSOC] ------" + name
                + " is initializing from initializePacaBodyStreamBolt " + messageUpstreamComponent);
        GetPacaBodyStreamBolt pacaStreamBolt = new GetPacaBodyStreamBolt();
        builder.setBolt(name, pacaStreamBolt)
                .shuffleGrouping(messageUpstreamComponent, "pcap_body_stream")
                .setNumTasks(1);
        return true;
    }

    private boolean initializePacaHeaderStreamBolt(String name) {
        String messageUpstreamComponent = "ParserBolt";
        System.out.println("[OpenSOC] ------" + name
                + " is initializing from initializePacaHeaderStreamBolt " + messageUpstreamComponent);
        GetPacaHeaderStreamBolt pacaStreamBolt = new GetPacaHeaderStreamBolt();
        builder.setBolt(name, pacaStreamBolt)
                .shuffleGrouping(messageUpstreamComponent, "pcap_header_stream")
                .setNumTasks(1);
        return true;
    }

    abstract void initializeTestingSpout(String component_name);

    abstract boolean initializeParsingBolt(String component_name);

    private boolean initializeIndexingBolt(String name) {
        try {

//            String messageUpstreamComponent = messageComponents
//                    .get(messageComponents.size() - 1);
            String messageUpstreamComponent = "ParserBolt";
            System.out.println("[OpenSOC] ------" + name
                    + " is initializing from " + messageUpstreamComponent);
            /**
             * adapter class
             * com.xyw55.indexing.adapters.ESTimedRotatingAdapter
             */
            Class loaded_class = Class.forName(configuration.getString("bolt.indexing.adapter"));
            IndexAdapter adapter = (IndexAdapter) loaded_class.newInstance();

            Map<String, String> settings = SettingsLoader.getConfigOptions((PropertiesConfiguration)configuration, "optional.settings.bolt.index.search.");

            if(settings != null && settings.size() > 0)
            {
                adapter.setOptionalSettings(settings);
                System.out.println("[OpenSOC] Index Bolt picket up optional settings:");
                SettingsLoader.printOptionalSettings(settings);
            }

            // dateFormat defaults to hourly if not specified
            String dateFormat = "yyyy.MM.dd.hh";
            if (configuration.containsKey("bolt.indexing.timestamp")) {
                dateFormat = configuration.getString("bolt.indexing.timestamp");
            }
            TelemetryIndexingBolt indexing_bolt = new TelemetryIndexingBolt()
                    .withIndexIP(configuration.getString("es.ip"))
                    .withIndexPort(configuration.getInt("es.port"))
                    .withClusterName(configuration.getString("es.clustername"))
                    .withIndexName(configuration.getString("bolt.indexing.indexname"))
                    .withIndexTimestamp(dateFormat)
                    .withDocumentName(
                            configuration.getString("bolt.indexing.documentname"))
                    .withBulk(configuration.getInt("bolt.indexing.bulk"))
                    .withIndexAdapter(adapter)
                    .withMetricConfiguration(configuration);

            builder.setBolt(name, indexing_bolt,
                    configuration.getInt("bolt.indexing.parallelism.hint"))
                    .fieldsGrouping(messageUpstreamComponent, "message",
                            new Fields("key"))
                    .setNumTasks(configuration.getInt("bolt.indexing.num.tasks"));

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return true;
    }
}
