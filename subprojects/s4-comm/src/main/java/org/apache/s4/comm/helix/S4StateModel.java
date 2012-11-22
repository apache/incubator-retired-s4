package org.apache.s4.comm.helix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

@StateModelInfo(states = { "LEADER,STANDBY" }, initialState = "OFFLINE")
public class S4StateModel extends StateModel
{
  private static Logger logger = LoggerFactory.getLogger(S4StateModel.class);

  private final String streamName;
  private final String partitionId;

  public S4StateModel(String partitionName)
  {
    String[] parts = partitionName.split("_");
    this.streamName = parts[0];
    this.partitionId = parts[1];
  }

  @Transition(from = "OFFLINE", to = "STANDBY")
  public void becomeLeaderFromOffline(Message msg, NotificationContext context)
  {
    logger.info("Transitioning from " + msg.getFromState() + " to "
        + msg.getToState() + "for " + msg.getPartitionName());
  }

  @Transition(from = "STANDBY", to = "LEADER")
  public void becomeLeaderFromStandby(Message msg, NotificationContext context)
  {
    logger.info("Transitioning from " + msg.getFromState() + " to "
        + msg.getToState() + "for " + msg.getPartitionName());
  }

  @Transition(from = "LEADER", to = "STANDBY")
  public void becomeStandbyFromLeader(Message msg, NotificationContext context)
  {
    logger.info("Transitioning from " + msg.getFromState() + " to "
        + msg.getToState() + "for " + msg.getPartitionName());
  }

  @Transition(from = "STANDBY", to = "OFFLINE")
  public void becomeOfflineFromStandby(Message msg, NotificationContext context)
  {
    logger.info("Transitioning from " + msg.getFromState() + " to "
        + msg.getToState() + "for " + msg.getPartitionName());
  }

}
