package spinoco.fs2.zk

import org.apache.zookeeper.Watcher.Event.KeeperState

/**
  * Created by pach on 07/05/16.
  */
object ZkClientState extends Enumeration {
  val Disconnected
      , SyncConnected
      , AuthFailed
      , ConnectedReadOnly
      , SASLAuthenticated
      , Expired
      , Closed
      = Value

  def fromZk(s:KeeperState):ZkClientState.Value = {
    (s: @unchecked) match { //suppressing states no longer supported by Zookeeper
      case KeeperState.Disconnected => Disconnected
      case KeeperState.SyncConnected => SyncConnected
      case KeeperState.AuthFailed => AuthFailed
      case KeeperState.ConnectedReadOnly => ConnectedReadOnly
      case KeeperState.SaslAuthenticated => SASLAuthenticated
      case KeeperState.Expired => Expired
    }
  }
}