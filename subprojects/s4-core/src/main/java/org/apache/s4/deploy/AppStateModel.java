package org.apache.s4.deploy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.s4.core.util.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(states = { "ONLINE,OFFLINE" }, initialState = "OFFLINE")
public class AppStateModel extends StateModel {
	private static Logger logger = LoggerFactory.getLogger(AppStateModel.class);
	private final String appName;
	private DeploymentManager deploymentManager;

	public AppStateModel(DeploymentManager deploymentManager, String appName) {
		this.deploymentManager = deploymentManager;
		this.appName = appName;
	}

	@Transition(from = "OFFLINE", to = "ONLINE")
	public void deploy(Message message, NotificationContext context)
			throws Exception {
		logger.info("Deploying app:"+ appName);
		HelixManager manager = context.getManager();
		ConfigAccessor configAccessor = manager.getConfigAccessor();
		AppConfig config = createAppConfig(manager, configAccessor);
		deploymentManager.deploy(config);
		logger.info("Deployed app:"+ appName);

	}

	private AppConfig createAppConfig(HelixManager manager,
			ConfigAccessor configAccessor) {
		ConfigScopeBuilder builder = new ConfigScopeBuilder();
		ConfigScope scope = builder.forCluster(manager.getClusterName())
				.forResource(appName).build();
		String appURI = configAccessor.get(scope,
				DeploymentManager.S4R_URI);
		String clusterName = manager.getClusterName();
		String appClassName = null;
		List<String> customModulesNames = new ArrayList<String>();
		List<String> customModulesURIs = new ArrayList<String>();
		Map<String, String> namedParameters = new HashMap<String, String>();
		AppConfig config = new AppConfig(clusterName, appClassName, appURI,
				customModulesNames, customModulesURIs, namedParameters);
		return config;
	}

	@Transition(from = "OFFLINE", to = "ONLINE")
	public void undeploy(Message message, NotificationContext context)
			throws Exception {
		logger.info("Undeploying app:"+ appName);
		HelixManager manager = context.getManager();
		ConfigAccessor configAccessor = manager.getConfigAccessor();
		AppConfig config = createAppConfig(manager, configAccessor);
		deploymentManager.undeploy(config);
		logger.info("Undeploying app:"+ appName);

	}

}
