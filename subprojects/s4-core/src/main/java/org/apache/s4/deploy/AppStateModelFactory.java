package org.apache.s4.deploy;

import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.s4.comm.util.ArchiveFetcher;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class AppStateModelFactory extends StateModelFactory<AppStateModel> {

    private final ArchiveFetcher fetcher;

    @Inject
    public AppStateModelFactory(ArchiveFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public AppStateModel createNewStateModel(String partitionName) {
        return new AppStateModel(partitionName, fetcher);
    }

}
