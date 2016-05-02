package spinoco.fs2

import fs2._
import fs2.Async

import scala.concurrent.duration._

/**
  * Created by pach on 01/05/16.
  */
package object zk {

  /**
    * Creates a zookeeper client. Typically the application has only one clients available.
    *
    * This Stream emits only once providing guarded ZkClient, after session was successfully established.
    *
    * Note that there is no specific `close` functionality. The client is terminated when the resulting Stream terminates.
    *
    *
    * @param ensemble         Zookeeper ensemble Uri .i.e. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
    *                         or "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" if chrooted under /app/a
    *
    * @param credentials      If Zookeeper ensemble requires authentication, credentials may be passed in here.
    *
    * @param allowReadOnly    True, indicates that if ensemble loses majority, the client will switch to readonly mode instead
    *                         of failing.
    * @tparam F
    * @return
    */
  def client[F[_]:Async](
    ensemble:String
    , credentials:Option[(String, Chunk.Bytes)] = None
    , allowReadOnly:Boolean = false
    , timeout: FiniteDuration = 10.seconds
  ): Stream[F,ZkClient[F]] =
    ZkClient(ensemble,credentials,allowReadOnly, timeout)






}
