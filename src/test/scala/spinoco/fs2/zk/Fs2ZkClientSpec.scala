package spinoco.fs2.zk

import java.util.concurrent.{Executors, ScheduledExecutorService}

import fs2.Stream._
import fs2._
import fs2.util.Task
import org.scalatest.concurrent.{Eventually, TimeLimitedTests}
import org.scalatest.{FreeSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.SpanSugar._

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

//  override def runTest(testName: String, args: Args): Status = {
//    println("Starting " + testName)
//    try super.runTest(testName, args)
//    finally println("Finished " + testName)
//  }

  implicit val S: Strategy = Strategy.fromFixedDaemonPool(8,"fs2-zk-spec")
  implicit val Sch: ScheduledExecutorService =  Executors.newScheduledThreadPool(4)

  def standaloneServer:Stream[Task,ZkSpecServer[Task]] =
    ZkSpecServer.startStandalone[Task]()

  def clientTo(server:ZkSpecServer[Task]):Stream[Task,ZkClient[Task]] = {
    eval(server.clientAddress) flatMap { address =>
      client[Task](s"127.0.0.1:${address.getPort}") flatMap {
        case Left(state) => Stream.fail(new Throwable(s"Failed to connect to server: $state"))
        case Right(zkC) => emit(zkC)
      }
    }
  }


  /**
    * Creates single server and connects client to it
    */
  def standaloneServerAndClient:Stream[Task,(ZkSpecServer[Task], ZkClient[Task])] =
    standaloneServer flatMap { zkS => clientTo(zkS).map(zkS -> _) }



}
