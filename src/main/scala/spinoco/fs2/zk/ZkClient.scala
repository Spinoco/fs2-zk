package spinoco.fs2.zk

import fs2._
import fs2.Stream._

import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.concurrent.duration.FiniteDuration


trait ZkClient[F[_]] {

  /**
    * Id of zookeeper client session
    */
  def sessionId:F[Long]

  /**
    * Create ZkNode.
    *
    * @param node           Node to create
    * @param data           If specified, data that has to be stored at `node`. MAximum chun size is 1MB
    * @param acl            ACLs applied for the node
    * @param createMode     Creation Mode of the node
    */
  def create(node:ZkNode, createMode:ZkCreateMode, data:Option[Chunk.Bytes], acl:List[ZkACL] ):F[String]

  /**
    * Delete node at given patth. Optionally specify version to delete node if node's version matches supplied version
    */
  def delete(node:ZkNode, version:Option[Int]):F[Unit]

  /**
    * Returns immediately once the check if supplied ZkNode exists at given path is performed at server.
    * Evaluates to Some(stats), if the ZkNode exists or to None, if it does not.
    */
  def existsNow(node:ZkNode):F[Option[ZkStat]]

  /**
    * Returns a stream of stats, that may be used to verify whether given node exists or not.
    * Returned stream is discrete, so it only emits when the information is updated (i.e. node added, modified or deleted).
    */
  def exists(node:ZkNode):Stream[F,Option[ZkStat]]

  /**
    * Returns a node's acl.
    * Evaluates to None, if the node does not exists or to Some(acl) with current acls of the node (possibly empty)
    */
  def aclOf(node:ZkNode):F[Option[List[ZkACL]]]

  /**
    * Returns children of given node. Returns immediately once server answers.
    * If the node does not exists  this evaluates to None, otherwise this evaluates to the list of all children
    */
  def childrenNowOf(node:ZkNode):F[Option[(List[ZkNode], ZkStat)]]

  /**
    * Returns a Stream of list of children, of supplied node. This stream is discrete, so it will emit
    * only when the children list is altered, that means child is added or deleted. It won't update itself when the
    * children was updated.
    *
    * Note that if node does not exists, this emits None.
    */
  def childrenOf(node:ZkNode):Stream[F,Option[List[ZkNode]]]



  /**
    * Returns data of the specified node. Possibly evals to None if either node does not exists, or no data are set.
    */
  def dataNowOf(node:ZkNode):F[Option[(Chunk.Bytes, ZkStat)]]

  /**
    * Returns discrete stream of data for the supplied node. Emits None if node cannot be found or there are no Data available.
    *
    */
  def dataOf(node:ZkNode):Stream[F,Option[(Chunk.Bytes, ZkStat)]]



  /**
    * Sets the data on given ZkNode. If the node does not exists, this evaluates to None.
    * Also yields to None, if supplied version does not matches the version of the node.
    */
  def setDataOf(node:ZkNode, data:Option[Chunk.Bytes], version:Option[Int]):F[Option[ZkStat]]

  /**
    * Sets the access list for the supplied node. Evaluates to None if the node does not exists or
    * if the supplied version won't match the version of the node.
    */
  def setAclOf(node:ZkNode, acl:List[ZkACL], version:Option[Int]):F[Option[ZkStat]]

  /**
    * Performs atomic operations. That means either None or all supplied operations will succeed.
    *
    * @param ops
    * @return
    */
  def atomic(ops:List[ZkOp]):F[Unit]

}



object ZkClient {


  def apply[F[_]](
    ensemble:String
    , credentials:Option[(String, Chunk.Bytes)]
    , allowReadOnly:Boolean
    , timeout: FiniteDuration
  )(implicit F: Async[F]): Stream[F,ZkClient[F]] = {
    Stream.bracket(F.async[ZooKeeper] { cb =>
      new ZooKeeper(ensemble,timeout.toMillis,impl.connectionWatcher(cb),allowReadOnly)
    })(
      zk => eval(impl.makeClient(zk))
      , zk => F.suspend(zk.close())
    )
  }


  object impl {

    def connectionWatcher(cb: Either[Throwable,ZooKeeper] => Unit):Watcher = {
      new Watcher {
        def process(event: WatchedEvent): Unit = ???
      }
      ???
    }


    def makeClient[F[_]](zk:ZooKeeper)(implicit F: Async[F]):F[ZkClient[F]] = {
      ???
    }

  }


}