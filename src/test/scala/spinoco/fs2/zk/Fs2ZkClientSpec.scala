package spinoco.fs2.zk

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import fs2.Stream._
import fs2._
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.time.SpanSugar._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers


/**
  * Created by pach on 14/05/16.
  */
class Fs2ZkClientSpec extends AnyFreeSpec
  with Matchers
  with TimeLimitedTests
  with Eventually {

  implicit def global: IORuntime = IORuntime.global

  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  def standaloneServer:Stream[IO, ZkSpecServer[IO]] =
    ZkSpecServer.startStandalone[IO]()

  def clientTo(server:ZkSpecServer[IO]): Stream[IO,ZkClient[IO]] = {
    eval(server.clientAddress) flatMap { address =>
      Stream.resource(ZkClient.instance[IO](s"127.0.0.1:${address.getPort}"))
    }
  }


  /**
    * Creates single server and connects client to it
    */
  def standaloneServerAndClient:Stream[IO,(ZkSpecServer[IO], ZkClient[IO])] =
    standaloneServer flatMap { zkS => clientTo(zkS).map(zkS -> _) }



}
