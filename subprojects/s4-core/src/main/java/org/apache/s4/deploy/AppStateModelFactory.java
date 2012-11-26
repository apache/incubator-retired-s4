package org.apache.s4.deploy;

import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.s4.core.Server;

import com.google.inject.Inject;
import com.google.inject.Singleton;
@Singleton
public class AppStateModelFactory extends StateModelFactory<AppStateModel>
{
  private final Server server;
  
  @Inject
  public AppStateModelFactory(Server server){
    this.server = server;
    
  }
  @Override
  public AppStateModel createNewStateModel(String partitionName)
  {
    return new AppStateModel(server,partitionName);
  }

}
