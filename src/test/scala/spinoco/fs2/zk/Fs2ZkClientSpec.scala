package spinoco.fs2.zk

import cats.effect.IO
import fs2.Stream._
import fs2._
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.SpanSugar._
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.ExecutionContext

/**
  * Created by pach on 14/05/16.
  */
class Fs2ZkClientSpec extends FreeSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with TimeLimitedTests
  with Eventually {

  val timeLimit = 90.seconds

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = timeLimit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 25, workers = 1)


  implicit val EC: ExecutionContext = ExecutionContext.global

  def standaloneServer:Stream[IO, ZkSpecServer[IO]] =
    ZkSpecServer.startStandalone[IO]()

  def clientTo(server:ZkSpecServer[IO]): Stream[IO,ZkClient[IO]] = {
    eval(server.clientAddress) flatMap { address =>
      client[IO](s"127.0.0.1:${address.getPort}") flatMap {
        case Left(state) => Stream.raiseError(new Throwable(s"Failed to connect to server: $state"))
        case Right(zkC) => emit(zkC)
      }
    }
  }


  /**
    * Creates single server and connects client to it
    */
  def standaloneServerAndClient:Stream[IO,(ZkSpecServer[IO], ZkClient[IO])] =
    standaloneServer flatMap { zkS => clientTo(zkS).map(zkS -> _) }



}
