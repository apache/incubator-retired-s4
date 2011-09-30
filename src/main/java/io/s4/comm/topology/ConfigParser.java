package io.s4.comm.topology;

import io.s4.comm.topology.Cluster.ClusterType;
import io.s4.core.Stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


public class ConfigParser {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConfigParser.class);
	
	public ConfigParser() {
	}
	
	public Config parse(String configFilename) {
		Config config = null;

		Document document = createDocument(configFilename);
		NodeList topLevelNodeList = document.getChildNodes();
		for (int i = 0; i < topLevelNodeList.getLength(); i++) {
			Node node = topLevelNodeList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("config")) {
				config = processConfigElement(node);
			}
		}
		verifyConfig(config);
		return config;
	}
	
	private void verifyConfig(Config config) {
		if (config.getClusters().size() == 0) {
			throw new VerifyError("No clusters specified");
		}
		
		for (Cluster cluster : config.getClusters()) {
			verifyCluster(cluster);
			
		}
	}
	
	public void verifyCluster(Cluster cluster) {
		if (cluster.getNodes().size() == 0) {
			throw new VerifyError("No nodes in cluster " + cluster.getName());
		}
		
		Set<String> taskSet = new HashSet<String>();
		for (ClusterNode node : cluster.getNodes()) {
			if (taskSet.contains(node.getTaskId())) {
				throw new VerifyError("Duplicate task id " + node.getTaskId());
			}
			if (node.getTaskId() == null) {
				throw new VerifyError("Missing task id");
			}
			taskSet.add(node.getTaskId());			
		}
		
		if (cluster.getType().equals(ClusterType.S4)) {
			verifyS4Cluster(cluster);
		}
		else {
			verifyAdapterCluster(cluster);
		}
	}
	
	public void verifyS4Cluster(Cluster cluster) {
		/*
		 * rules:
		 * 1)	if any node has a partition id,
		 * 		a)	all must have partition ids
		 * 		b)	the partition ids must be 0-n, where n is the number of nodes
		 * 			minus 1
		*/
		int nodeCount = cluster.getNodes().size();
		Set<Integer> idSet = new HashSet<Integer>();
		for (ClusterNode node : cluster.getNodes()) {			
			int partitionId = node.getPartition();
			if (partitionId == -1) {
				throw new VerifyError("No partition specified on node " + node.getTaskId());
			}
			if (partitionId < 0 || partitionId > (nodeCount-1)) {
				throw new VerifyError("Bad partition specified " + partitionId);
			}
			if (idSet.contains(new Integer(partitionId))) {
				throw new VerifyError("Duplicate partition in cluster: " + partitionId);
			}
			idSet.add(partitionId);
			
			if (node.getPort() == -1) {
				throw new VerifyError("Missing port number on node " + node.getTaskId());
			}
		}
		
		if (idSet.size() != nodeCount && idSet.size() != 0) {
			throw new VerifyError("Bad partition ids in cluster " + idSet);
		}		
	}
	
	public void verifyAdapterCluster(Cluster cluster) {
		for (ClusterNode node : cluster.getNodes()) {			
			if (node.getPartition() != -1) {
				throw new VerifyError("Cannot specify partition for adapter node");
			}
		}
	}
	
	public Config processConfigElement(Node configElement) {
	    String version = ((Element)configElement).getAttribute("version");
	    if (version == null ||     version.length() > 0) {
	        version = "-1";
	    }
	    
		NodeList nodeList = configElement.getChildNodes();
		
		Config config = new Config(version);
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("cluster")) {
				config.addCluster(processClusterElement(node));
			}
		}
		
		return config;
	}
	
	public Cluster processClusterElement(Node clusterElement) {
		Cluster cluster = new Cluster();
		 
		String mode = ((Element)clusterElement).getAttribute("mode");
		if (mode != null) {
			cluster.setMode(mode);
		}
		String name = ((Element)clusterElement).getAttribute("name");
		if (name != null) {
			cluster.setName(name);
		}
		String typeString = ((Element)clusterElement).getAttribute("type");
		if (typeString != null) {
			if (typeString.equals("adapter")) {
				cluster.setType(ClusterType.ADAPTER);
			}
			else if (typeString.equals("s4")) {
				cluster.setType(ClusterType.S4);
			}			
		}
		
		NodeList nodeList = clusterElement.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("node")) {
				cluster.addNode(processClusterNodeElement(node));
			}
		}
		return cluster;
	}

	public ClusterNode processClusterNodeElement(Node clusterNodeElement) {
		int partition = -1;
		int port = 0;
		String machineName = null;
		String taskId = null;
		
		NodeList nodeList = clusterNodeElement.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			
			if (node.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			
			if (node.getNodeName().equals("partition")) {
				try {
					partition = Integer.parseInt(getElementContentText(node));
					
				}
				catch (NumberFormatException nfe) {
					throw new VerifyError("Bad partition specified " + getElementContentText(node));
				}
			}
			else if (node.getNodeName().equals("port")) {
				try {
					port = Integer.parseInt(getElementContentText(node));
					
				}
				catch (NumberFormatException nfe) {
					throw new VerifyError("Bad port specified " + getElementContentText(node));
				}
			}
			else if (node.getNodeName().equals("machine")) {
				machineName = getElementContentText(node);
			}
			else if (node.getNodeName().equals("taskId")) {
				taskId = getElementContentText(node);
			}
		}
		
		return new ClusterNode(partition, port, machineName, taskId);
	}
	
	private static Document createDocument(String configFilename) {
		try {
			Document document;
			// Get a JAXP parser factory object
			javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory
					.newInstance();
			// Tell the factory what kind of parser we want
			dbf.setValidating(false);
			dbf.setIgnoringComments(true);
			dbf.setIgnoringElementContentWhitespace(true);
			// Use the factory to get a JAXP parser object
			javax.xml.parsers.DocumentBuilder parser = dbf.newDocumentBuilder();

			// Tell the parser how to handle errors. Note that in the JAXP API,
			// DOM parsers rely on the SAX API for error handling
			parser.setErrorHandler(new org.xml.sax.ErrorHandler() {
				public void warning(SAXParseException e) {
					logger.warn("WARNING: " + e.getMessage(), e);
				}

				public void error(SAXParseException e) {
					logger.error("ERROR: " + e.getMessage(),e);
				}

				public void fatalError(SAXParseException e) throws SAXException {
					logger.error("FATAL ERROR: " + e.getMessage(), e);
					throw e; // re-throw the error
				}
			});

			// Finally, use the JAXP parser to parse the file. This call returns
			// A Document object. Now that we have this object, the rest of this
			// class uses the DOM API to work with it; JAXP is no longer
			// required.
			InputStream is = getResourceStream(configFilename);
			if(is == null){
				throw new RuntimeException("Unable to find config file:"+ configFilename);
			}
			document = parser.parse(is);
			return document;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public String getElementContentText(Node node) {
		if (node.getNodeType() != Node.ELEMENT_NODE) {
			return "";
		}
		NodeList children = node.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (child.getNodeType() == Node.TEXT_NODE) {
				return child.getNodeValue();
			}
		}
		
		return "";
	}
	
	public static void main(String[] args) {
		ConfigParser parser = new ConfigParser();
		Config config = parser.parse(args[0]);
	}

	private static InputStream getResourceStream(String configfile) {
		try {
			File f = new File(configfile);
			if (f.exists()) {
				if (f.isFile()) {
					return new FileInputStream(configfile);
				} else {
					throw new RuntimeException("configFile " + configfile
							+ "  is not a regular file:");
				}
			}
			InputStream is = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(configfile);
			if (is != null) {
				return is;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}
	
	public class VerifyError extends RuntimeException {
		public VerifyError(String message) {
			super(message);
		}
	}
}
