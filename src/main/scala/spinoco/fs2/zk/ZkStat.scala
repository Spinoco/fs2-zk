package spinoco.fs2.zk

import java.time.LocalDateTime

/**
  * Stat structure of zookeeper
  * @param createZxId               The zxid of the change that caused this znode to be created.
  * @param modifiedZxId             The zxid of the change that last modified this znode.
  * @param createdAt                The time in milliseconds from epoch when this znode was created.
  * @param modifiedAt               The time in milliseconds from epoch when this znode was last modified.
  * @param version                  The number of changes to the data of this znode.
  * @param childVersion             The number of changes to the children of this znode.
  * @param aclVersion               The number of changes to the ACL of this znode.
  * @param ephemeralOwnerSession    The session id of the owner of this znode if the znode is an ephemeral node.
  *                                 If it is not an ephemeral node, it will be zero.
  * @param dataLength               The length of the data field of this znode.
  * @param numChildren              The number of children of this znode.
  */
case class ZkStat(
  createZxId:Long
  , modifiedZxId:Long
  , createdAt:LocalDateTime
  , modifiedAt:LocalDateTime
  , version:Int
  , childVersion:Int
  , aclVersion:Int
  , ephemeralOwnerSession:Long
  , dataLength:Int
  , numChildren:Int
)
