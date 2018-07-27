package spinoco.fs2.zk

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.{Properties, UUID}

import cats.effect.Effect
import org.apache.zookeeper.server.{ServerCnxnFactory, ServerConfig, ZooKeeperServer}
import fs2._
import fs2.Stream._
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import org.apache.zookeeper.server.quorum.QuorumPeerConfig

trait ZkSpecServer[F[_]] {

  /** Performs a shutdown of the server **/
  def shutdown:F[Unit]

  /** allows to startup server that was previously shut down **/
  def startup:F[Unit]

  /** address where the clients has to contact this server **/
  def clientAddress:F[InetSocketAddress]

}


/**
  * Server of zookeeper used in specifications.
  * This is used to launch the server instance, perform the tests and then tear it down.
  */
object ZkSpecServer {


  /**
    * Creates new Zk Server
    * Note that server must be explicitly started by `startup`.
    *
    * @tparam F
    * @return
    */
  def standalone[F[_]](port:Int = 10000) (implicit F:Effect[F]): Stream[F,ZkSpecServer[F]] = {
    eval(F.suspend(F.pure(Files.createTempDirectory(s"ZK_${UUID.randomUUID()}")))) flatMap { dataDir =>
      def buildServer: F[ZkSpecServer[F]] =
        F.delay {
          val props = impl.mkProps(Seq(port), 1, dataDir)
          impl.mkZkServer(impl.mkServerConfig(props))
        }


      def cleanup(zkS:ZkSpecServer[F]) : F[Unit] =
        F.flatMap(zkS.shutdown) { _ => TestUtil.removeRecursively(dataDir) }

      Stream.bracket(buildServer)(cleanup)
    }
  }

  /**
    * Like `standalone` except it will start the server immediately.
    */
  def startStandalone[F[_]](port:Int = 10000)(implicit F:Effect[F]): Stream[F,ZkSpecServer[F]] = {
    standalone(port).flatMap(zks => eval_(zks.startup) ++ emit(zks))
  }


  object impl {

    def mkProps(
      ensemble: Seq[Int]
      , serverId: Int
      , dataDir: Path
    ):Properties = {
      val peers = ensemble.zipWithIndex.map { case (port, idx) =>
        s"server.${idx + 1}" -> s"127.0.0.1:${port+1}:${port+2}"
      }.toMap


      val props =
        makeProps(Map(
          "dataDir" -> dataDir.toString
          , "clientPort" -> ensemble(serverId -1 ).toString
        ) ++ peers)


      props
    }

    def mkServerConfig(props:Properties):ServerConfig = {
      val qCfg = new QuorumPeerConfig()
      qCfg.parseProperties(props)
      val cfg = new ServerConfig()
      cfg.readFrom(qCfg)
      cfg
    }


    def mkZkServer[F[_]](config: ServerConfig)(implicit F:Effect[F]):ZkSpecServer[F] = {
      val idx = new AtomicInteger(0)
      val runningServer = new AtomicReference[ZooKeeperServer]()
      new ZkSpecServer[F] {

        def clientAddress: F[InetSocketAddress] = F.suspend(F.pure(config.getClientPortAddress))

        def startup: F[Unit] = F.map(configureServer(config)) { server =>
          runningServer.set(server)
          val cnxnFactory = ServerCnxnFactory.createFactory
          cnxnFactory.configure(config.getClientPortAddress, config.getMaxClientCnxns)
          val run = new Runnable { override def run(): Unit = {
            cnxnFactory.startup(server)
            cnxnFactory.join()

          }}
          new Thread(run,s"fs2-zk-server-standalone-${idx.incrementAndGet()}").start()
        }


        def shutdown: F[Unit] = F.suspend { F.pure {
          val server = runningServer.get()
          runningServer.set(null)
          if (server != null)  { server.getServerCnxnFactory.shutdown(); server.shutdown() }
        }}
      }
    }

    def makeProps(cfg:Map[String,String]):Properties = {
      val props = new Properties()
      cfg.foreach { case (k,v) => props.put(k,v) }
      props
    }

    def configureServer[F[_]](config:ServerConfig)(implicit F:Effect[F]): F[ZooKeeperServer] = F.delay {
      val zkServer: ZooKeeperServer = new ZooKeeperServer

      val txnLog = new FileTxnSnapLog(new File(config.getDataDir), new File(config.getDataLogDir))
      zkServer.setTxnLogFactory(txnLog)
      zkServer.setTickTime(config.getTickTime)
      zkServer.setMinSessionTimeout(config.getMinSessionTimeout)
      zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout)
      zkServer
    }



  }

}
