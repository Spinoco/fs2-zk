package spinoco.fs2.zk

import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream._
import fs2._
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.time.SpanSugar._
import org.scalatest.{FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.ExecutionContext

/**
  * Created by pach on 14/05/16.
  */
class Fs2ZkClientSpec extends FreeSpec
  with ScalaCheckDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {

  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  val EC: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  implicit val cs: ContextShift[IO] = IO.contextShift(EC)
  implicit val timeout: Timer[IO] = IO.timer(EC)

  def standaloneServer:Stream[IO, ZkSpecServer[IO]] =
    ZkSpecServer.startStandalone[IO]()

  def clientTo(server:ZkSpecServer[IO]): Stream[IO,ZkClient[IO]] = {
    eval(server.clientAddress) flatMap { address =>
      Stream.resource(client[IO](s"127.0.0.1:${address.getPort}"))
    }
  }


  /**
    * Creates single server and connects client to it
    */
  def standaloneServerAndClient:Stream[IO,(ZkSpecServer[IO], ZkClient[IO])] =
    standaloneServer flatMap { zkS => clientTo(zkS).map(zkS -> _) }



}
