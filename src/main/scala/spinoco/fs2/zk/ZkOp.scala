package spinoco.fs2.zk

import fs2.Chunk


object ZkOp {

  /**
    * Creates given node
    *
    * @param node           Node to create
    * @param data           If specified, data that has to be stored at `node`. MAximum chun size is 1MB
    * @param acl            ACLs applied for the node
    * @param createMode     Creation Mode of the node
    */
  case class Create(node:ZkNode, createMode:ZkCreateMode, data:Option[Chunk.Bytes], acl:List[ZkACL]) extends ZkOp

  /**
    * Deletes given node
    */
  case class Delete(node:ZkNode) extends ZkOp

  /**
    * Sets data on given node
    */
  case class SetData(node:ZkNode, data:Option[Chunk.Bytes]) extends ZkOp

  /**
    * Performs check that given node is of supplied version
    */
  case class Check(node:ZkNode, version: Int) extends ZkOp

}


sealed trait ZkOp
