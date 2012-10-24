/**
 * Integration between S4 and YARN (Hadoop). Implements a client, application master, and container for running S4 nodes in YARN clusters.
 * 
 * YARN is leveraged to start S4 nodes. 
 * 
 * The deployment of S4 applications remains unchanged: S4 applications are published through ZooKeeper, and they are automatically downloaded by S4 nodes.
 * 
 * 
 */
package org.apache.s4.tools.yarn;

