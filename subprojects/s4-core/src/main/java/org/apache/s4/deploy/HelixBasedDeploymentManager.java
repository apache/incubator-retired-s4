package org.apache.s4.deploy;

import org.apache.s4.core.Server;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class HelixBasedDeploymentManager implements DeploymentManager
{
  private final Server server;
  boolean deployed = false;
  private final String clusterName;

  @Inject
  public HelixBasedDeploymentManager(
      @Named("s4.cluster.name") String clusterName,
      @Named("s4.cluster.zk_address") String zookeeperAddress,
      @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
      @Named("s4.cluster.zk_connection_timeout") int connectionTimeout,
      Server server)
  {
    this.clusterName = clusterName;
    this.server = server;

  }

  @Override
  public void start()
  {
   /* File s4r = new File(
        "/Users/kgopalak/Documents/projects/s4/incubator-s4-helix/myApp/build/libs/myApp.s4r");
    String appName = "myApp";
    try
    {
      App loaded = server.loadApp(s4r, "myApp");
      server.startApp(loaded, appName, clusterName);
    } catch (Exception e)
    {
      e.printStackTrace();
    }*/
  }

}
