package com.xyw55.index.interfaces;

import org.json.simple.JSONObject;

import java.util.Map;

public interface IndexAdapter {

	boolean initializeConnection(String ip, int port, String cluster_name,
								 String index_name, String document_name, int bulk, String date_format) throws Exception;

	int bulkIndex(JSONObject raw_message);

	void setOptionalSettings(Map<String, String> settings);
}
