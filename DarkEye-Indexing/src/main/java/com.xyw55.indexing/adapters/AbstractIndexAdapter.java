package com.xyw55.indexing.adapters;

import com.xyw55.index.interfaces.IndexAdapter;
import com.xyw55.indexing.AbstractIndexingBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class AbstractIndexAdapter implements IndexAdapter, Serializable{
	
	protected static final Logger _LOG = LoggerFactory
			.getLogger(AbstractIndexingBolt.class);


	

	abstract public boolean initializeConnection(String ip, int port,
			String cluster_name, String index_name, String document_name,
			int bulk, String date_format) throws Exception;

}
