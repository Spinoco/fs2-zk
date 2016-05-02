package spinoco.fs2.zk

/**
  * ACL Entry of ZooKeeper Node
  */
case class ZkACL (
  permission: ZkACL.Permission.Value
  , scheme: String
  , entity: String
)


object ZkACL {

  object Permission extends Enumeration {
      val Read, Write, Admin, Create, Delete = Value
  }

}