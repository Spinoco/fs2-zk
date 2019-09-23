package spinoco.fs2.zk

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JList}

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Deferred
import cats.{Applicative, Monad}
import fs2.Stream._
import fs2._
import fs2.concurrent.{Signal, SignallingRef}
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Id, Stat}
import spinoco.fs2.zk.ListConverters.{toScalaList, toJavaList}
import spinoco.fs2.zk.ZkACL.Permission

import scala.concurrent.duration.FiniteDuration
import scala.util.Success


trait ZkClient[F[_]] {

  /**
    * Id of zookeeper client session
    */
  def sessionId:F[Long]

  /**
    * Create ZkNode. Evaluates to name of the node created. That name can be used to construct full node
    * name in case of sequential nodes.
    *
    * @param node           Node to create
    * @param data           If specified, data that has to be stored at `node`. Maximum chunk size is 1MB
    * @param acl            ACLs applied for the node
    * @param createMode     Creation Mode of the node
    */
  def create(node: ZkNode, createMode: ZkCreateMode, data: Option[Chunk[Byte]], acl: List[ZkACL] ): F[ZkNode]

  /**
    * Delete node at given patth. Optionally specify version to delete node if node's version matches supplied version
    */
  def delete(node: ZkNode, version: Option[Int]): F[Unit]

  /**
    * Returns immediately once the check if supplied ZkNode exists at given path is performed at server.
    * Evaluates to Some(stats), if the ZkNode exists or to None, if it does not.
    */
  def existsNow(node: ZkNode): F[Option[ZkStat]]

  /**
    * Returns a stream of stats, that may be used to verify whether given node exists or not.
    * Returned stream is discrete, so it only emits when the information is updated (i.e. node added, modified or deleted).
    */
  def exists(node: ZkNode): Stream[F, Option[ZkStat]]

  /**
    * Returns a node's acl.
    * Evaluates to None, if the node does not exists or to Some(acl) with current acls of the node (possibly empty)
    */
  def aclOf(node: ZkNode): F[Option[List[ZkACL]]]

  /**
    * Returns children of given node. Returns immediately once server answers.
    * If the node does not exists  this evaluates to None, otherwise this evaluates to the list of all children, possibly empty
    */
  def childrenNowOf(node: ZkNode): F[Option[(List[ZkNode], ZkStat)]]

  /**
    * Returns a Stream of list of children, of supplied node. This stream is discrete, so it will emit
    * only when the children list is altered, that means child is added or deleted. It won't update itself when the
    * children was updated.
    *
    * Note that if node does not exists, this emits None.
    */
  def childrenOf(node: ZkNode): Stream[F, Option[(List[ZkNode], ZkStat)]]



  /**
    * Returns data of the specified node. Possibly evals to None if either node does not exists, or no data are set.
    */
  def dataNowOf(node: ZkNode): F[Option[(Chunk[Byte], ZkStat)]]

  /**
    * Returns discrete stream of data for the supplied node. Emits None if node cannot be found or there are no Data available.
    *
    */
  def dataOf(node: ZkNode): Stream[F, Option[(Chunk[Byte], ZkStat)]]

  /**
    * Sets the data on given ZkNode. If the node does not exists, this evaluates to None.
    * Also yields to None, if supplied version does not matches the version of the node.
    */
  def setDataOf(node: ZkNode, data:Option[Chunk[Byte]], version:Option[Int]): F[Option[ZkStat]]

  /**
    * Sets the access list for the supplied node. Evaluates to None if the node does not exists or
    * if the supplied version won't match the version of the node.
    */
  def setAclOf(node:ZkNode, acl: List[ZkACL], version: Option[Int]): F[Option[ZkStat]]

  /**
    * Performs atomic operations. That means either None or all supplied operations will succeed.
    *
    * @param ops
    * @return
    */
  def atomic(ops: List[ZkOp]): F[List[ZkOpResult]]

  /**
    * Provides stream of client state. This allows fine-tuned monitoring of ZkClient, primarily
    * when the client disconnects or expired.
    */
  def clientState:Stream[F,ZkClientState.Value]

}



object ZkClient {

  @inline def apply[F[_]](implicit instance: ZkClient[F]): ZkClient[F] = instance

  def instance[F[_]: ConcurrentEffect : ContextShift](
    ensemble: String
    , credentials: Option[(String, Chunk[Byte])]
    , allowReadOnly: Boolean
    , timeout: FiniteDuration
  ): Resource[F, ZkClient[F]] = {
    def make = {
      Deferred[F, Signal[F, ZkClientState.Value]].flatMap { deferred =>
      Sync[F].delay(new ZooKeeper(ensemble, timeout.toMillis.toInt, impl.connectionWatcher(deferred), allowReadOnly)).flatMap { zk =>
      impl.createClient[F](allowReadOnly)(deferred, zk).map { client =>
        (zk, client)
      }}}
    }

    Resource.make(make)({ case (zk, _) => Sync[F].delay(zk.close()).attempt.void })
    .flatMap { case (_, client) => Resource.pure(client)  }
  }


  object impl {

    /**
      * Builds a stream of the ZkClient that emits once.
      *
      *
      * Initially this awaits client being connected and terminates on Left if there is some other error due the connect.
      *
      * This emits on Right only if state went to `SyncConnected` or to `ConnectedReadOnly` in case `allowReadOnly` is set to true.
      *
      */
    def createClient[F[_]: ConcurrentEffect: ContextShift](
      allowReadOnly: Boolean
    )(
      signal: Deferred[F, Signal[F, ZkClientState.Value]]
      , zk: ZooKeeper
    ): F[ZkClient[F]] =
      signal.get.flatMap { ref =>
      ref.get.flatMap {
        case ZkClientState.SyncConnected  => makeClient(zk, ref)
        case ZkClientState.ConnectedReadOnly if allowReadOnly => makeClient(zk, ref)
        case other => Sync[F].raiseError(new InvalidZookeeperState(other))
      }}


    def connectionWatcher[F[_] : ConcurrentEffect](deferred: Deferred[F, Signal[F, ZkClientState.Value]]): Watcher = {
      new Watcher {
        val signal  = new AtomicReference[Option[SignallingRef[F, ZkClientState.Value]]](None)
        def process(event: WatchedEvent): Unit = {
          // process is somehow complicated, but essentially on the first signal of the state
          // we create the SignallingRef, and feed it that signal.
          // Atomic reference is preventing double setting that ref and making sure we get consistent state at the end.

          event.getType match {
            case EventType.None =>
              val state  = ZkClientState.fromZk(event.getState)
              def spin: F[Unit] =
                signal.get() match {
                  case None =>
                    SignallingRef[F, ZkClientState.Value](state).flatMap { ref =>
                      if (signal.compareAndSet(None, Some(ref))) deferred.complete(ref)
                      else spin
                    }

                  case Some(ref) =>
                    ref.set(state)
                }

              Effect[F].toIO(spin).unsafeRunSync()

            case _ => ()
          }
        }
      }
    }

    def stringCallBack(cb: Either[Throwable, String] => Unit): StringCallback = {
      new StringCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
          zkResult(rc)(cb(Right(name)))(cb)
        }
      }
    }

    def zkResult(rc: Int)(success: => Unit)(failed: Either[Throwable, Nothing] => Unit): Unit = {
      KeeperException.Code.get(rc) match {
        case null => failed(Left(new Throwable(s"Unknown Zookeeper result code: $rc")))
        case KeeperException.Code.OK => success
        case other => failed(Left(KeeperException.create(other)))
      }
    }

    def makeClient[F[_]: ConcurrentEffect: ContextShift](
      zk:ZooKeeper
      , signal:Signal[F, ZkClientState.Value]
    ): F[ZkClient[F]] = {
      Sync[F].delay {
        new ZkClient[F] {
          def sessionId: F[Long] = Sync[F].suspend(Applicative[F].pure(zk.getSessionId))
          def dataNowOf(node: ZkNode): F[Option[(Chunk[Byte], ZkStat)]] = impl.dataNowOf(zk,node)
          def dataOf(node: ZkNode): Stream[F, Option[(Chunk[Byte], ZkStat)]] = impl.dataOf(zk,node)
          def setAclOf(node: ZkNode, acl: List[ZkACL], version: Option[Int]): F[Option[ZkStat]] = impl.setAclOf(zk,node, acl, version)
          def setDataOf(node: ZkNode, data: Option[Chunk[Byte]], version: Option[Int]): F[Option[ZkStat]] = impl.setDataOf(zk, node, data, version)
          def existsNow(node: ZkNode): F[Option[ZkStat]] = impl.existsNow(zk,node)
          def delete(node: ZkNode, version: Option[Int]): F[Unit] = impl.delete(zk, node, version)
          def atomic(ops: List[ZkOp]): F[List[ZkOpResult]] = impl.atomic(zk,ops)
          def aclOf(node: ZkNode): F[Option[List[ZkACL]]] = impl.aclOf(zk,node)
          def create(node: ZkNode, createMode: ZkCreateMode, data: Option[Chunk[Byte]], acl: List[ZkACL]): F[ZkNode] =  impl.create(zk,node,createMode, data, acl)
          def exists(node: ZkNode): Stream[F, Option[ZkStat]] = impl.exists(zk,node)
          def childrenOf(node: ZkNode): Stream[F, Option[(List[ZkNode], ZkStat)]] = impl.childrenOf(zk,node)
          def childrenNowOf(node: ZkNode): F[Option[(List[ZkNode], ZkStat)]] = impl.childrenNowOf(zk,node)
          def clientState: Stream[F, ZkClientState.Value] = signal.discrete
        }
      }
    }

    def create[F[_]: Async](zk: ZooKeeper,node: ZkNode, createMode: ZkCreateMode, data: Option[Chunk[Byte]], acl: List[ZkACL]): F[ZkNode] = {
      Async[F].async[String] { cb =>
        zk.create(node.path,data.map(_.toArray).orNull,fromZkACL(acl), zkCreateMode(createMode), stringCallBack(cb), null)
      } map ZkNode.apply
    }

    def dataNowOf[F[_]: Async](zk: ZooKeeper, node:ZkNode): F[Option[(Chunk[Byte], ZkStat)]] =
      Async[F].async { cb => zk.getData(node.path,false, mkDataCallBack(cb), null) }

    def dataOf[F[_] : ConcurrentEffect](zk: ZooKeeper, node:ZkNode): Stream[F, Option[(Chunk[Byte], ZkStat)]] =
      mkZkStream { case (cb, watcher) => zk.getData(node.path, watcher, mkDataCallBack(cb), null) }

    def setDataOf[F[_]: Async: ContextShift](zk: ZooKeeper,node: ZkNode, data: Option[Chunk[Byte]], version: Option[Int]): F[Option[ZkStat]] =
      Async[F].async[Option[ZkStat]] { cb => zk.setData(node.path, data.map(_.toArray).orNull,version.getOrElse(-1), mkStatCallBack(cb), null) } <* implicitly[ContextShift[F]].shift

    def exists[F[_]: ConcurrentEffect](zk: ZooKeeper, node: ZkNode): Stream[F, Option[ZkStat]] =
      mkZkStream { case (cb,watcher) => zk.exists(node.path, watcher, mkStatCallBack(cb), null) }

    def existsNow[F[_]: Async: ContextShift](zk: ZooKeeper, node: ZkNode): F[Option[ZkStat]] =
      Async[F].async[Option[ZkStat]] { cb => zk.exists(node.path, false, mkStatCallBack(cb), null) } <* implicitly[ContextShift[F]].shift

    def childrenOf[F[_]: ConcurrentEffect](zk: ZooKeeper, node: ZkNode): Stream[F, Option[(List[ZkNode], ZkStat)]] = {
      def children:Stream[F, Option[(List[ZkNode], ZkStat)]] =
        mkZkStream { case (cb,watcher) =>
          zk.getChildren(node.path, watcher, mkChildren2CallBack(node)(cb), null)
        }

      def go: Stream[F, Option[(List[ZkNode], ZkStat)]] = {
        children flatMap {
          case None =>
            emit(None) ++ (exists[F](zk, node).find(_.isDefined) flatMap { _ => go })
          case result => emit(result)
        }
      }
      go
    }

    def childrenNowOf[F[_]](zk: ZooKeeper, node: ZkNode)(implicit F: Async[F]): F[Option[(List[ZkNode], ZkStat)]] =
      F.async { cb =>  zk.getChildren(node.path, false, mkChildren2CallBack(node)(cb), null) }

    def aclOf[F[_]](zk: ZooKeeper, node: ZkNode)(implicit F: Async[F]): F[Option[List[ZkACL]]] =
      F.async { cb => zk.getACL(node.path,null,mkACLCallBack(cb),null) }

    def setAclOf[F[_]: ContextShift : Async](zk: ZooKeeper, node: ZkNode, acl: List[ZkACL], version: Option[Int]): F[Option[ZkStat]] =
      Async[F].async[Option[ZkStat]] { cb => zk.setACL(node.path, fromZkACL(acl), version.getOrElse(-1), mkStatCallBack(cb),null)  } <* implicitly[ContextShift[F]].shift

    def delete[F[_]](zk: ZooKeeper, node: ZkNode, version: Option[Int])(implicit F: Async[F]): F[Unit] =
      F.async { cb => zk.delete(node.path, version.getOrElse(-1), mkVoidCallBack(cb), null) }

    def atomic[F[_]](zk: ZooKeeper,ops: List[ZkOp])(implicit F: Async[F]): F[List[ZkOpResult]] =
      F.async { cb => zk.multi(toOp(ops),mkMultiCallBack(cb),null) }

    def mkDataCallBack(cb: Either[Throwable,Option[(Chunk[Byte], ZkStat)]] => Unit): DataCallback = {
      new  DataCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
          def result:Option[(Chunk[Byte], ZkStat)] = {
            if (data == null) None
            else {
              Some((Chunk.bytes(data, 0, data.length), zkStats(stat)))
            }
          }
          def failure(err:Either[Throwable,Nothing]):Unit = {
            err match {
              case Left(_:KeeperException.NoNodeException) => cb(Right(None))
              case _ => cb(err)
            }
          }
          zkResult(rc)(cb(Right(result)))(failure)
        }
      }
    }

    def mkStatCallBack(cb: Either[Throwable, Option[ZkStat]] => Unit): StatCallback = {
      new StatCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
          def failure(err:Either[Throwable,Nothing]):Unit = {
            err match {
              case Left(_:KeeperException.NoNodeException) => cb(Right(None))
              case _ => cb(err)
            }
          }
          
          zkResult(rc)(cb(Right(Some(zkStats(stat)))))(failure)
        }
      }
    }

    def mkChildren2CallBack(parent: ZkNode)(cb: Either[Throwable, Option[(List[ZkNode], ZkStat)]] => Unit): Children2Callback = {
      new Children2Callback {
        def processResult(rc: Int, path: String, ctx: scala.Any, children: JList[String], stat:Stat): Unit = {
          def result:Option[(List[ZkNode], ZkStat)] = {
            if (children == null) Some(List.empty -> zkStats(stat))
            else {
              val result = toScalaList(children).map(parent / _).collect { case Success(zkn) => zkn }
              Some(result -> zkStats(stat))
            }
          }
          def failure(err:Either[Throwable,Nothing]):Unit = {
            err match {
              case Left(_:KeeperException.NoNodeException) => cb(Right(None))
              case _ => cb(err)
            }
          }
          zkResult(rc)(cb(Right(result)))(failure)
        }
      }
    }

    def mkACLCallBack(cb: Either[Throwable, Option[List[ZkACL]]] => Unit): ACLCallback = {
      new ACLCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any, acl: JList[ACL], stat: Stat): Unit = {
          def result:Option[List[ZkACL]] = {
            if (acl == null) Some(List.empty)
            else Some(toZkACL(acl))
          }
          def failure(err:Either[Throwable,Nothing]):Unit = {
            err match {
              case Left(_:KeeperException.NoNodeException) => cb(Right(None))
              case _ => cb(err)
            }
          }
          zkResult(rc)(cb(Right(result)))(failure)
        }
      }
    }

    def mkVoidCallBack(cb:Either[Throwable, Unit] => Unit): VoidCallback = {
      new VoidCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any): Unit = {
          zkResult(rc)(cb(Right(())))(cb)
        }
      }
    }

    def mkMultiCallBack(cb:Either[Throwable, List[ZkOpResult]] => Unit): MultiCallback = {
      new MultiCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any, opResults: JList[OpResult]): Unit = {
          zkResult(rc)(cb(Right(fromOpResult(opResults))))(cb)
        }
      }
    }

    def mkWatcher[F[_]: ConcurrentEffect]: F[(Watcher, F[Unit])] = {
      Deferred[F, Unit].map { ref =>
        val watcher = new Watcher {
          def process(event: WatchedEvent): Unit = Effect[F].runAsync(ref.complete(()))(IO.fromEither).unsafeRunSync()
        }
        watcher -> ref.get
      }
    }

    def mkZkStream[F[_]: ConcurrentEffect, CB <: AsyncCallback, O](register: (Either[Throwable,O] => Unit, Watcher) => Unit): Stream[F,O] = {
      def readF: F[(O, F[Unit])] = {
        Monad[F].flatMap(mkWatcher[F]){ case (watcher, awaitWatch) =>
        Async[F].async[(O, F[Unit])] { cb =>
          register({ r => cb(r.map(_ -> awaitWatch))}, watcher)
        }}
      }

      def go: Stream[F,O] = {
        eval(readF) flatMap { case (o,awaitWatch) =>
          emit(o) ++ eval_(awaitWatch) ++ go
        }
      }
      go
    }


    def zkCreateMode(mode:ZkCreateMode): CreateMode = {
      mode match {
        case ZkCreateMode(false,false) => CreateMode.PERSISTENT
        case ZkCreateMode(false,true) => CreateMode.PERSISTENT_SEQUENTIAL
        case ZkCreateMode(true,false) => CreateMode.EPHEMERAL
        case ZkCreateMode(true,true) =>  CreateMode.EPHEMERAL_SEQUENTIAL
      }
    }

    def fromZkACL(acls: List[ZkACL]): JList[ACL] = toJavaList {
      acls.map { zkAcl =>
        new ACL(zkAcl.permission.value,new Id(zkAcl.scheme, zkAcl.entity) )
      }
    }

    def toZkACL(acls: JList[ACL]): List[ZkACL] = {
      toScalaList(acls).map { acl =>
        ZkACL(Permission(acl.getPerms),acl.getId.getScheme, acl.getId.getId)
      }
    }

    def zkStats(stat: Stat):ZkStat = {
      ZkStat(
        createZxId = stat.getCzxid
        , modifiedZxId = stat.getMzxid
        , createdAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(stat.getCtime), ZoneId.systemDefault())
        , modifiedAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(stat.getMtime), ZoneId.systemDefault())
        , version = stat.getVersion
        , childVersion = stat.getCversion
        , aclVersion = stat.getAversion
        , ephemeralOwnerSession = stat.getEphemeralOwner
        , dataLength = stat.getDataLength
        , numChildren = stat.getNumChildren
      )
    }

    def fromOpResult(results: JList[OpResult]): List[ZkOpResult] = {
      toScalaList(results).map {
        case create: OpResult.CreateResult => ZkOpResult.CreateResult(create.getPath)
        case _: OpResult.DeleteResult => ZkOpResult.DeleteResult
        case data: OpResult.SetDataResult => ZkOpResult.SetDataResult(zkStats(data.getStat))
        case _: OpResult.CheckResult => ZkOpResult.CheckResult
        case err:OpResult.ErrorResult => ZkOpResult.ErrorResult(err.getErr)
      }
    }

    def toOp(ops: List[ZkOp]): JList[Op] = toJavaList {
      ops.map {
        case ZkOp.Create(node, mode, data, acl) => Op.create(node.path,data.map(_.toArray).orNull,fromZkACL(acl),zkCreateMode(mode))
        case ZkOp.Delete(node, version) => Op.delete(node.path, version.getOrElse(-1))
        case ZkOp.SetData(node, data, version) => Op.setData(node.path, data.map(_.toArray).orNull, version.getOrElse(-1))
        case ZkOp.Check(node, version) => Op.check(node.path, version.getOrElse(-1))
      }
    }

  }


}