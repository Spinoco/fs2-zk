package spinoco.fs2.zk

import java.net.{InetSocketAddress, Socket}
import cats.effect.Sync
import fs2._
import fs2.Stream._

import scala.annotation.tailrec
import scala.concurrent.duration._
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
        impl.mkDockerZkServer(port)
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

    def mkDockerZkServer[F[_]](zkPort: Int)(implicit F:Sync[F]):ZkSpecServer[F] = {
      new ZkSpecServer[F] {
        
        // Use incremental container naming
        private val containerName = s"zk-server-${containerCounter.incrementAndGet()}"

        def clientAddress: F[InetSocketAddress] = 
          F.pure(new InetSocketAddress("localhost", zkPort))

        def startup: F[Unit] = F.delay {
          // Get ZooKeeper version from environment variable
          val zkVersion = sys.env.getOrElse("ZK_VERSION", "3.4.10")

          // Start new ZooKeeper container
          // Map host port to ZooKeeper's default port 2181 inside the container
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

          // Wait until container is fully removed and port is released
          waitForContainerRemoval()
          waitForPortRelease(zkPort)
        }
        
        private def waitForZkReady(port: Int): Unit = {
          val maxRetries = 30
          val retryDelay = 1000 // 1 second

          @tailrec def attemptConnection(attempt: Int): Unit = {
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

        /**
          * Waits for the Docker container to be fully removed.
          *
          * @param maxRetries Maximum number of attempts to check container removal
          * @param retryDelay Delay between retry attempts
          */
        def waitForContainerRemoval(maxRetries: Int = 30, retryDelay: FiniteDuration = 500.millis): Unit = {
           /*
            * Recursively checks if the Docker container still exists.
            * Uses `docker ps -a` to verify container removal.
            *
            * @param attempt Current attempt number
            * @throws RuntimeException if container is not removed after max attempts
            */
          @tailrec def checkContainer(attempt: Int): Unit = {
            if (attempt > maxRetries) {
              throw new RuntimeException(s"Container $containerName failed to be removed after $maxRetries attempts")
            } else {
               val containerExists = try {
                // Check if container still exists
                val result = s"docker ps -a -q -f name=$containerName".!!.trim
                result.nonEmpty
              } catch {
                case _: Exception =>
                  // Command failed, assume container is gone
                  false
              }

              if (containerExists) {
                // Container still exists, wait and retry
                Thread.sleep(retryDelay.toMillis)
                checkContainer(attempt + 1)
              }
              // Container is gone, we're done
            }
          }

          checkContainer(1)
        }

        /**
          * Waits for the specified port to be released after container shutdown.
          * Attempts to bind to the port to verify it's available.
          *
          * @param port The port number to check for availability
          * @param maxRetries Maximum number of attempts to check port availability
          * @param retryDelay Delay between retry attempts
          */
        def waitForPortRelease(port: Int, maxRetries: Int = 20, retryDelay: FiniteDuration = 500.millis): Unit = {

           /*
            * Recursively checks if the port is available by attempting to bind to it.
            * If binding succeeds, the port is free. If it fails with BindException,
            * the port is still in use and we retry.
            *
            * @param attempt Current attempt number
            */
          @tailrec def checkPort(attempt: Int): Unit = {
            if (attempt > maxRetries) {
              // Port still in use after max retries, but don't fail - just log warning
              println(s"Warning: Port $port may still be in use after container removal")
            } else {
              try {
                // Try to bind to the port - if successful, it's free
                val serverSocket = new java.net.ServerSocket(port)
                serverSocket.close()
                // Port is free, we're done
              } catch {
                case _: java.net.BindException =>
                  // Port still in use, wait and retry
                  Thread.sleep(retryDelay.toMillis)
                  checkPort(attempt + 1)
                case _: Exception =>
                  // Other error, assume port will be free
              }
            }
          }

          checkPort(1)
        }
      }
    }

  }

}
