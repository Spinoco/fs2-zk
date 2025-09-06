package spinoco.fs2.zk

import java.net.{InetSocketAddress, Socket}
import cats.effect.Sync
import fs2._
import fs2.Stream._

import scala.sys.process._
import scala.util.Try

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
  def standalone[F[_]](port:Int = 10000) (implicit F:Sync[F]): Stream[F,ZkSpecServer[F]] = {
    def buildServer: F[ZkSpecServer[F]] =
      F.delay {
        impl.mkDockerZkServer()
      }

    def cleanup(zkS:ZkSpecServer[F]) : F[Unit] =
      zkS.shutdown

    Stream.bracket(buildServer)(cleanup)
  }

  /**
    * Like `standalone` except it will start the server immediately.
    */
  def startStandalone[F[_]](port:Int = 10000)(implicit F:Sync[F]): Stream[F,ZkSpecServer[F]] = {
    standalone(port).flatMap(zks => exec(zks.startup) ++ emit(zks))
  }


  object impl {
    private val containerCounter = new java.util.concurrent.atomic.AtomicInteger(0)

    def mkDockerZkServer[F[_]]()(implicit F:Sync[F]):ZkSpecServer[F] = {
      new ZkSpecServer[F] {
        
        // Use incremental container naming
        private val containerName = s"zk-server-${containerCounter.incrementAndGet()}"
        private val zkPort = 2181

        def clientAddress: F[InetSocketAddress] = 
          F.pure(new InetSocketAddress("localhost", zkPort))

        def startup: F[Unit] = F.delay {
          // Get ZooKeeper version from environment variable
          val zkVersion = sys.env.getOrElse("ZK_VERSION", "3.4.10")
          
          // Start new ZooKeeper container
          val cmd = Seq(
            "docker", "run", "-d",
            "--name", containerName,
            "-p", s"$zkPort:2181",
            s"zookeeper:$zkVersion"
          )
          
          cmd.!!
          
          // Wait for ZooKeeper to be ready
          waitForZkReady(zkPort)
        }
        

        def shutdown: F[Unit] = F.delay {
          Thread.sleep(1000) // Give some time before we shutdown server 
          Try { s"docker stop $containerName".!! }
          Try { s"docker rm $containerName".!! }
          ()
        }
        
        private def waitForZkReady(port: Int): Unit = {
          val maxRetries = 30
          val retryDelay = 1000 // 1 second
          
          def attemptConnection(attempt: Int): Unit = {
            if (attempt > maxRetries) {
              throw new RuntimeException(s"ZooKeeper failed to start after $maxRetries attempts")
            } else {
              try {
                val socket = new Socket()
                socket.connect(new InetSocketAddress("localhost", port), 1000)
                socket.close()
                // Successfully connected, we're done
              } catch {
                case _: Exception =>
                  Thread.sleep(retryDelay)
                  attemptConnection(attempt + 1)
              }
            }
          }
          
          attemptConnection(1)
        }
      }
    }

  }

}
