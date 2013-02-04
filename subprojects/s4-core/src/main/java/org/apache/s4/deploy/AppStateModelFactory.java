package org.apache.s4.deploy;

import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.Server;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class AppStateModelFactory extends StateModelFactory<AppStateModel> {
    private final DeploymentManager deploymentManager;

    @Inject
    public AppStateModelFactory(DeploymentManager deploymentManager, ArchiveFetcher fetcher) {
        this.deploymentManager = deploymentManager;

    }

    @Override
    public AppStateModel createNewStateModel(String partitionName) {
        return new AppStateModel(deploymentManager,partitionName);
    }

}
