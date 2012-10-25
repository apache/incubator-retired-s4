/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.tools.yarn;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.s4.tools.DefineCluster;
import org.apache.s4.tools.Deploy;
import org.apache.s4.tools.Tools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

/**
 * Client for S4 application submission to YARN.
 * 
 * <p>
 * It connects to an existing YARN infrastructure (based on configuration files in HADOOP_CONF_DIR environment
 * variables), then launches an S4 application master, passing all relevant configuration. The application master then
 * asks resources from the resource manager and creates S4 nodes on those allocated resources, with the appropriate
 * configuration.
 * </p>
 * 
 * <p>
 * <u>Implementation notes from YARN example: </u>
 * </p>
 * <p>
 * To submit an application, a client first needs to connect to the <code>ResourceManager</code> aka ApplicationsManager
 * or ASM via the {@link ClientRMProtocol}. The {@link ClientRMProtocol} provides a way for the client to get access to
 * cluster information and to request for a new {@link ApplicationId}.
 * <p>
 * 
 * <p>
 * For the actual job submission, the client first has to create an {@link ApplicationSubmissionContext}. The
 * {@link ApplicationSubmissionContext} defines the application details such as {@link ApplicationId} and application
 * name, user submitting the application, the priority assigned to the application and the queue to which this
 * application needs to be assigned. In addition to this, the {@link ApplicationSubmissionContext} also defines the
 * {@link ContainerLaunchContext} which describes the <code>Container</code> with which the {@link S4ApplicationMaster}
 * is launched.
 * </p>
 * 
 * <p>
 * The {@link ContainerLaunchContext} in this scenario defines the resources to be allocated for the
 * {@link S4ApplicationMaster}'s container, the local resources (jars, configuration files) to be made available and the
 * environment to be set for the {@link S4ApplicationMaster} and the commands to be executed to run the
 * {@link S4ApplicationMaster}.
 * <p>
 * 
 * <p>
 * Using the {@link ApplicationSubmissionContext}, the client submits the application to the
 * <code>ResourceManager</code> and then monitors the application by requesting the <code>ResourceManager</code> for an
 * {@link ApplicationReport} at regular time intervals. In case of the application taking too long, the client kills the
 * application by submitting a {@link KillApplicationRequest} to the <code>ResourceManager</code>.
 * </p>
 * 
 */
public class S4YarnClient extends YarnClientImpl {

    private static final String HADOOP_CONF_DIR_ENV = System.getenv("HADOOP_CONF_DIR");

    private static final ImmutableSet<String> YARN_CONF_FILES = ImmutableSet.of("core-site.xml", "hdfs-site.xml",
            "yarn-site.xml", "mapred-site.xml");

    private static Logger logger = LoggerFactory.getLogger(S4YarnClient.class);

    // Configuration
    private Configuration conf;

    YarnArgs yarnArgs;

    private int amMemory;

    private final long clientStartTime = System.currentTimeMillis();

    /**
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) {

        YarnArgs yarnArgs = new YarnArgs();
        logger.info("S4YarnClient args = " + Arrays.toString(args));

        Tools.parseArgs(yarnArgs, args);
        boolean result = false;
        try {

            YarnConfiguration yarnConfig = new YarnConfiguration();

            if (Strings.isNullOrEmpty(HADOOP_CONF_DIR_ENV)) {
                logger.error("You must define HADOOP_CONF_DIR environment variable");
                System.exit(1);
            }
            File confDir = new File(HADOOP_CONF_DIR_ENV);
            if (!(confDir.listFiles(new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    return YARN_CONF_FILES.contains(pathname.getName());
                }
            }).length == 4)) {
                logger.error("The {} directory must contain files [core,hdfs,yarn,mapred]-site.xml");
                System.exit(1);
            }

            for (String fileName : YARN_CONF_FILES) {
                yarnConfig.addResource(new Path(new File(HADOOP_CONF_DIR_ENV, fileName).toURI()));
            }

            S4YarnClient client = new S4YarnClient(yarnArgs, yarnConfig);
            result = client.run(false);
        } catch (Throwable t) {
            logger.error("Error running Client", t);
            System.exit(1);
        }
        if (result) {
            logger.info("Application completed successfully");
            System.exit(0);
        }
        logger.error("Application failed to complete successfully");
        System.exit(1);
    }

    public S4YarnClient(YarnArgs yarnArgs, Configuration conf) throws Exception {
        this.yarnArgs = yarnArgs;
        this.conf = conf;
        init(this.conf);
    }

    /**
     * Main run function for the client
     * 
     * @return true if application completed successfully
     * @throws IOException
     */
    public boolean run(boolean testMode) throws IOException {
        logger.info("Running Client");
        start();

        YarnClusterMetrics clusterMetrics = super.getYarnClusterMetrics();
        logger.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = super.getNodeReports();
        logger.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            logger.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId() + ", nodeAddress"
                    + node.getHttpAddress() + ", nodeRackName" + node.getRackName() + ", nodeNumContainers"
                    + node.getNumContainers() + ", nodeHealthStatus" + node.getNodeHealthStatus());
        }

        QueueInfo queueInfo = super.getQueueInfo(yarnArgs.queue);
        logger.info("Queue info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity="
                + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount="
                + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = super.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                logger.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl="
                        + userAcl.name());
            }
        }

        // Get a new application id
        GetNewApplicationResponse newApp = super.getNewApplication();
        ApplicationId appId = newApp.getApplicationId();

        // TODO get min/max resource capabilities from RM and change memory ask if needed
        // If we do not have min/max, we may not be able to correctly request
        // the required resources from the RM for the app master
        // Memory ask has to be a multiple of min and less than max.
        // Dump out information about cluster capability as seen by the resource manager
        int minMem = newApp.getMinimumResourceCapability().getMemory();
        int maxMem = newApp.getMaximumResourceCapability().getMemory();
        logger.info("Min mem capability of resources in this cluster " + minMem);
        logger.info("Max mem capability of resources in this cluster " + maxMem);

        // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
        // a multiple of the min value and cannot exceed the max.
        // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
        if (amMemory < minMem) {
            logger.info("AM memory specified below min threshold of cluster. Using min value." + ", specified="
                    + amMemory + ", min=" + minMem);
            amMemory = minMem;
        } else if (amMemory > maxMem) {
            logger.info("AM memory specified above max threshold of cluster. Using max value." + ", specified="
                    + amMemory + ", max=" + maxMem);
            amMemory = maxMem;
        }

        // Create launch context for app master
        logger.info("Setting up application submission context for ASM");
        ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

        // set the application id
        appContext.setApplicationId(appId);
        // set the application name
        appContext.setApplicationName(yarnArgs.appName);

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        if (!(new File(yarnArgs.s4Dir).isDirectory())) {
            logger.error("Invalid s4 directory : " + yarnArgs.s4Dir);
            System.exit(1);
        }

        // TODO avoid depending on source distribution paths
        File[] classPathFiles = new File(yarnArgs.s4Dir + "/subprojects/s4-yarn/build/install/s4-yarn/lib").listFiles();
        FileSystem fs = FileSystem.get(conf);

        for (int i = 0; i < classPathFiles.length; i++) {
            Path dest = copyToLocalResources(appId, fs, localResources, classPathFiles[i]);
            logger.info("Copied classpath resource " + classPathFiles[i].getAbsolutePath() + " to "
                    + dest.toUri().toString());
        }

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);

        // Set the necessary security tokens as needed
        // amContainer.setContainerTokens(containerToken);

        // Set the env variables to be setup in the env where the application master will be run
        logger.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(':');
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(":./log4j.properties");

        env.put("CLASSPATH", classPathEnv.toString());

        amContainer.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        logger.info("Setting up app master command");

        // vargs.add("${JAVA_HOME}" + "/bin/java");
        vargs.add("java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // vargs.add("-Xdebug");
        // vargs.add("-Xrunjdwp:transport=dt_socket,address=8888,server=y");
        // Set Application Master class name
        vargs.add(S4ApplicationMaster.class.getName());
        // Set params for Application Master
        vargs.add("--container_memory " + String.valueOf(yarnArgs.containerMemory));
        vargs.add("--num_containers " + String.valueOf(yarnArgs.numContainers));
        vargs.add("--priority " + String.valueOf(yarnArgs.priority));
        if (!yarnArgs.extraModulesClasses.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("-extraModulesClasses ");
            ListIterator<String> it = yarnArgs.extraModulesClasses.listIterator();
            while (it.hasNext()) {
                sb.append(it.next() + (it.hasNext() ? "," : ""));
            }
            vargs.add(sb.toString());
        }
        if (!yarnArgs.extraNamedParameters.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("-namedStringParameters ");
            ListIterator<String> it = yarnArgs.extraNamedParameters.listIterator();
            while (it.hasNext()) {
                sb.append(it.next() + (it.hasNext() ? "," : ""));
            }
            vargs.add(sb.toString());
        }

        vargs.add("-c " + String.valueOf(yarnArgs.cluster));
        vargs.add("-zk " + String.valueOf(yarnArgs.zkString));
        if (testMode) {
            vargs.add("-test");
        }

        if (yarnArgs.debug) {
            vargs.add("--debug");
        }

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        logger.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        amContainer.setResource(capability);

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        // TODO - what is the range for priority? how to decide?
        pri.setPriority(yarnArgs.priority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(yarnArgs.queue);
        // Set the user submitting this application
        // TODO can it be empty?
        appContext.setUser(yarnArgs.user);

        // Define a new cluster for the application
        String[] defineClusterArgs = { "-cluster=" + yarnArgs.cluster, "-nbTasks=" + yarnArgs.nbTasks,
                "-flp=" + yarnArgs.flp, "-zk=" + yarnArgs.zkString };
        DefineCluster.main(defineClusterArgs);

        // Deply the application to the new cluster
        String[] deployApplicationArgs = { "-s4r=" + yarnArgs.s4rPath, "-cluster=" + yarnArgs.cluster,
                "-appName=" + yarnArgs.cluster, "-shutdown=false", "-zk=" + yarnArgs.zkString };
        Deploy.main(deployApplicationArgs);

        logger.info("Submitting application to ASM");
        super.submitApplication(appContext);

        // TODO
        // Try submitting the same request again
        // app submission failure?

        // Monitor the application (TODO: optional?)

        return monitorApplication(appId);

    }

    private Path copyToLocalResources(ApplicationId appId, FileSystem fs, Map<String, LocalResource> localResources,
            File file) throws IOException {
        Path src = new Path(file.getAbsolutePath());

        // TODO use home directory + appId / appName?
        Path dst = new Path(new Path(fs.getHomeDirectory(), "/app-" + appId.getId()), file.getName());
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);
        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setType(LocalResourceType.FILE);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        // Set timestamp and length of file so that the framework
        // can do basic sanity checks for the local resource
        // after it has been copied over to ensure it is the same
        // resource the client intended to use with the application
        resource.setTimestamp(destStatus.getModificationTime());
        resource.setSize(destStatus.getLen());
        localResources.put(file.getName(), resource);
        return dst;
    }

    /**
     * Monitor the submitted application for completion. Kill application if time expires.
     * 
     * @param appId
     *            Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnRemoteException
     */
    private boolean monitorApplication(ApplicationId appId) throws YarnRemoteException {

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = super.getApplicationReport(appId);

            logger.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToken="
                    + report.getClientToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost="
                    + report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
                    + report.getRpcPort() + ", appStartTime=" + report.getStartTime() + ", yarnAppState="
                    + report.getYarnApplicationState().toString() + ", distributedFinalState="
                    + report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    logger.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    logger.info("Application did finished unsuccessfully." + " YarnState=" + state.toString()
                            + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                logger.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus="
                        + dsStatus.toString() + ". Breaking monitoring loop");
                return false;
            }

            if ((yarnArgs.timeout != -1) && (System.currentTimeMillis() > (clientStartTime + yarnArgs.timeout))) {
                logger.info("Reached client specified timeout for application. Killing application");
                forceKillApplication(appId);
                return false;
            }
        }

    }

    /**
     * Kill a submitted application by sending a call to the Applications Manager
     * 
     * @param appId
     *            Application Id to be killed.
     * @throws YarnRemoteException
     */
    private void forceKillApplication(ApplicationId appId) throws YarnRemoteException {
        super.killApplication(appId);
    }

}
