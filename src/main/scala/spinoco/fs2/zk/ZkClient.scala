package spinoco.fs2.zk

import cats.Monad
import cats.data.OptionT
import cats.effect._
import cats.effect.std.Dispatcher
import cats.implicits._
import fs2.Stream._
import fs2._
import fs2.concurrent.{Signal, SignallingRef}
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Id, Stat}
import scodec.bits.ByteVector
import spinoco.fs2.zk.ZkACL.Permission

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JList}
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{FiniteDuration, _}
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
  def create(node: ZkNode, createMode: ZkCreateMode, data: Option[ByteVector], acl: List[ZkACL] ): F[ZkNode]

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
  def dataNowOf(node: ZkNode): F[Option[(ByteVector, ZkStat)]]

  /**
    * Returns discrete stream of data for the supplied node. Emits None if node cannot be found or there are no Data available.
    *
    */
  def dataOf(node: ZkNode): Stream[F, Option[(ByteVector, ZkStat)]]

  /**
    * Sets the data on given ZkNode. If the node does not exists, this evaluates to None.
    * Also yields to None, if supplied version does not matches the version of the node.
    */
  def setDataOf(node: ZkNode, data:Option[ByteVector], version:Option[Int]): F[Option[ZkStat]]

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

  /**
   * Creates a zookeeper client. Typically, the application has only one client available.
   *
   * Note that there is no specific `close` functionality. The client is terminated when the resulting resource releases.
   *
   * @param ensemble         Zookeeper ensemble Uri .i.e. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   *                         or "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" if chrooted under /app/a
   * @param credentials      If Zookeeper ensemble requires authentication, credentials may be passed in here.
   * @param allowReadOnly    True, indicates that if ensemble loses majority, the client will switch to readonly mode instead
   *                         of failing.
   */
  def instance[F[_]: Async](
    ensemble: String
    , credentials: Option[(String, ByteVector)] = None
    , allowReadOnly: Boolean = false
    , timeout: FiniteDuration = 10.seconds
  ): Resource[F, ZkClient[F]] = {
    def make(implicit d: Dispatcher[F]) = {
      Deferred[F, Signal[F, ZkClientState.Value]].flatMap { deferred =>
      Sync[F].delay(new ZooKeeper(ensemble, timeout.toMillis.toInt, impl.connectionWatcher(deferred), allowReadOnly)).flatMap { zk =>
        val authF = OptionT.fromOption[F](credentials)
          .semiflatMap { case (scheme, auth) => Sync[F].delay(zk.addAuthInfo(scheme, auth.toArray)) }
          .getOrElse(())
        
        authF >> impl.createClient[F](allowReadOnly)(deferred, zk).map { client =>
          (zk, client)
        }
      }}
    }

    Dispatcher.parallel.flatMap { implicit dispatcher =>
      Resource.make(make)({ case (zk, _) =>
        // We are releasing the resource, close the client,
        // We can ignore any errors on close as we are closing anyway.
        Sync[F].delay(zk.close()).attempt.void
      })
      .map { case (_, client) => client }
    }
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
    def createClient[F[_]: Async: Dispatcher](
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


    def connectionWatcher[F[_] : Async: Dispatcher](deferred: Deferred[F, Signal[F, ZkClientState.Value]]): Watcher = {
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
                      if (signal.compareAndSet(None, Some(ref))) deferred.complete(ref).void
                      else spin
                    }

                  case Some(ref) =>
                    ref.set(state)
                }

              implicitly[Dispatcher[F]].unsafeRunAndForget(spin)

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

    def makeClient[F[_]: Async: Dispatcher](
      zk:ZooKeeper
      , signal:Signal[F, ZkClientState.Value]
    ): F[ZkClient[F]] = {
      Sync[F].delay {
        new ZkClient[F] {
          def sessionId: F[Long] = Sync[F].delay(zk.getSessionId)
          def dataNowOf(node: ZkNode): F[Option[(ByteVector, ZkStat)]] = impl.dataNowOf(zk,node)
          def dataOf(node: ZkNode): Stream[F, Option[(ByteVector, ZkStat)]] = impl.dataOf(zk,node)
          def setAclOf(node: ZkNode, acl: List[ZkACL], version: Option[Int]): F[Option[ZkStat]] = impl.setAclOf(zk,node, acl, version)
          def setDataOf(node: ZkNode, data: Option[ByteVector], version: Option[Int]): F[Option[ZkStat]] = impl.setDataOf(zk, node, data, version)
          def existsNow(node: ZkNode): F[Option[ZkStat]] = impl.existsNow(zk,node)
          def delete(node: ZkNode, version: Option[Int]): F[Unit] = impl.delete(zk, node, version)
          def atomic(ops: List[ZkOp]): F[List[ZkOpResult]] = impl.atomic(zk,ops)
          def aclOf(node: ZkNode): F[Option[List[ZkACL]]] = impl.aclOf(zk,node)
          def create(node: ZkNode, createMode: ZkCreateMode, data: Option[ByteVector], acl: List[ZkACL]): F[ZkNode] =  impl.create(zk,node,createMode, data, acl)
          def exists(node: ZkNode): Stream[F, Option[ZkStat]] = impl.exists(zk,node)
          def childrenOf(node: ZkNode): Stream[F, Option[(List[ZkNode], ZkStat)]] = impl.childrenOf(zk,node)
          def childrenNowOf(node: ZkNode): F[Option[(List[ZkNode], ZkStat)]] = impl.childrenNowOf(zk,node)
          def clientState: Stream[F, ZkClientState.Value] = signal.discrete
        }
      }
    }

    def create[F[_]: Async](zk: ZooKeeper,node: ZkNode, createMode: ZkCreateMode, data: Option[ByteVector], acl: List[ZkACL]): F[ZkNode] = {
      Async[F].async[String] { cb =>
        Sync[F].delay {
          zk.create(node.path,data.map(_.toArray).orNull,fromZkACL(acl), zkCreateMode(createMode), stringCallBack(cb), null)
          None
        }
      } map ZkNode.apply
    }

    def dataNowOf[F[_]: Async](zk: ZooKeeper, node:ZkNode): F[Option[(ByteVector, ZkStat)]] =
      Async[F].async { cb => 
        Sync[F].delay {
          zk.getData(node.path,false, mkDataCallBack(cb), null)
          None
        }
      }

    def dataOf[F[_] : Async: Dispatcher](zk: ZooKeeper, node:ZkNode): Stream[F, Option[(ByteVector, ZkStat)]] =
      mkZkStream { case (cb, watcher) => zk.getData(node.path, watcher, mkDataCallBack(cb), null) }

    def setDataOf[F[_]: Async](zk: ZooKeeper,node: ZkNode, data: Option[ByteVector], version: Option[Int]): F[Option[ZkStat]] =
      Async[F].async[Option[ZkStat]] { cb => 
        Sync[F].delay {
          zk.setData(node.path, data.map(_.toArray).orNull,version.getOrElse(-1), mkStatCallBack(cb), null)
          None
        }
      }

    def exists[F[_]: Async: Dispatcher](zk: ZooKeeper, node: ZkNode): Stream[F, Option[ZkStat]] =
      mkZkStream { case (cb,watcher) => zk.exists(node.path, watcher, mkStatCallBack(cb), null) }

    def existsNow[F[_]: Async](zk: ZooKeeper, node: ZkNode): F[Option[ZkStat]] =
      Async[F].async[Option[ZkStat]] { cb => 
        Sync[F].delay {
          zk.exists(node.path, false, mkStatCallBack(cb), null)
          None
        }
      }

    def childrenOf[F[_]: Async: Dispatcher](zk: ZooKeeper, node: ZkNode): Stream[F, Option[(List[ZkNode], ZkStat)]] = {
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
      F.async { cb => 
        Sync[F].delay {
          zk.getChildren(node.path, false, mkChildren2CallBack(node)(cb), null)
          None
        }
      }

    def aclOf[F[_]](zk: ZooKeeper, node: ZkNode)(implicit F: Async[F]): F[Option[List[ZkACL]]] =
      F.async { cb => 
        Sync[F].delay {
          zk.getACL(node.path,null,mkACLCallBack(cb),null)
          None
        }
      }

    def setAclOf[F[_]: Async](zk: ZooKeeper, node: ZkNode, acl: List[ZkACL], version: Option[Int]): F[Option[ZkStat]] =
      Async[F].async[Option[ZkStat]] { cb => 
        Sync[F].delay {
          zk.setACL(node.path, fromZkACL(acl), version.getOrElse(-1), mkStatCallBack(cb),null)
          None
        }
      }

    def delete[F[_]](zk: ZooKeeper, node: ZkNode, version: Option[Int])(implicit F: Async[F]): F[Unit] =
      F.async { cb => 
        Sync[F].delay {
          zk.delete(node.path, version.getOrElse(-1), mkVoidCallBack(cb), null)
          None
        }
      }

    def atomic[F[_]](zk: ZooKeeper,ops: List[ZkOp])(implicit F: Async[F]): F[List[ZkOpResult]] =
      F.async { cb => 
        Sync[F].delay {
          zk.multi(toOp(ops),mkMultiCallBack(cb),null)
          None
        }
      }

    def mkDataCallBack(cb: Either[Throwable,Option[(ByteVector, ZkStat)]] => Unit): DataCallback = {
      new  DataCallback {
        def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
          def result:Option[(ByteVector, ZkStat)] = {
            if (data == null) None
            else {
              Some((ByteVector(data), zkStats(stat)))
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
              val result = children.asScala.toList.map(parent / _).collect { case Success(zkn) => zkn}
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

    def mkWatcher[F[_]: Async: Dispatcher]: F[(Watcher, F[Unit])] = {
      Deferred[F, Unit].map { ref =>
        val watcher = new Watcher {
          def process(event: WatchedEvent): Unit = implicitly[Dispatcher[F]].unsafeRunAndForget(ref.complete(()).void)
        }
        watcher -> ref.get
      }
    }

    def mkZkStream[F[_]: Async: Dispatcher, CB <: AsyncCallback, O](register: (Either[Throwable,O] => Unit, Watcher) => Unit): Stream[F,O] = {
      def readF: F[(O, F[Unit])] = {
        Monad[F].flatMap(mkWatcher[F]){ case (watcher, awaitWatch) =>
        Async[F].async[(O, F[Unit])] { cb =>
          Sync[F].delay {
            register({ r => cb(r.map(_ -> awaitWatch))}, watcher)
            None
          }
        }}
      }

      def go: Stream[F,O] = {
        eval(readF) flatMap { case (o,awaitWatch) =>
          emit(o) ++ exec(awaitWatch) ++ go
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

    def fromZkACL(acls: List[ZkACL]): JList[ACL] = {
      acls.map { zkAcl =>
        new ACL(zkAcl.permission.value,new Id(zkAcl.scheme, zkAcl.entity) )
      }.asJava
    }

    def toZkACL(acls: JList[ACL]): List[ZkACL] = {
      acls.asScala.toList.map { acl =>
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
      results.asScala.toList.map {
        case create: OpResult.CreateResult => ZkOpResult.CreateResult(create.getPath)
        case _: OpResult.DeleteResult => ZkOpResult.DeleteResult
        case data: OpResult.SetDataResult => ZkOpResult.SetDataResult(zkStats(data.getStat))
        case _: OpResult.CheckResult => ZkOpResult.CheckResult
        case err:OpResult.ErrorResult => ZkOpResult.ErrorResult(err.getErr)
      }
    }

    def toOp(ops: List[ZkOp]): JList[Op] = {
      ops.map {
        case ZkOp.Create(node, mode, data, acl) => Op.create(node.path,data.map(_.toArray).orNull,fromZkACL(acl),zkCreateMode(mode))
        case ZkOp.Delete(node, version) => Op.delete(node.path, version.getOrElse(-1))
        case ZkOp.SetData(node, data, version) => Op.setData(node.path, data.map(_.toArray).orNull, version.getOrElse(-1))
        case ZkOp.Check(node, version) => Op.check(node.path, version.getOrElse(-1))
      }.asJava
    }

  }


}