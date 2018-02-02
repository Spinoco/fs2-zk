package spinoco.fs2.zk

import cats.effect.IO
import fs2.Stream._
import fs2._

import concurrent.duration._

class ZkClientSpec extends Fs2ZkClientSpec {


  "ZooKeeper Client" - {

    val node1 = ZkNode.parse("/n1").get

    def sleep1s = Sch.sleep_[IO](1.second)

    "create and delete Node" in {

      val result =
        standaloneServerAndClient.flatMap { case (zks, zkc) =>
          eval(zkc.create(node1, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))).map(Right(_)) ++
          eval(zkc.existsNow(node1)).map(Left(_))
        }.compile.toVector.unsafeRunSync()

      result should have size(2)
      result(0) shouldBe Right(node1)
      result(1).left.map(_.nonEmpty) shouldBe Left(true)

    }


    "receive updates about node created, updated and deleted" in {

      val result =
        standaloneServer.flatMap { zks =>
          val observe = clientTo(zks) flatMap { zkc => zkc.exists(node1) }
          val modify =
            sleep1s ++
              clientTo(zks) flatMap { zkc =>
                eval_(zkc.create(node1, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
                  sleep1s ++ eval_(zkc.setDataOf(node1, Some(Chunk.bytes(Array[Byte](1,2,3), 0, 3)), None)) ++
                  sleep1s ++ eval_(zkc.delete(node1, None))
            }

          observe concurrently modify
        }
        .map { _.map(_.dataLength) }
        .take(4).compile.toVector.unsafeRunTimed(5.seconds)

      result shouldBe Some(Vector(
        None
        , Some(0)
        , Some(3)
        , None
      ))

    }


    "Signals updates of the children" in {
      val nodeA = (node1 / "a").get
      val nodeB = (node1 / "b").get
      val nodeC = (node1 / "c").get
       val result =
         standaloneServer.flatMap { zks =>
           val observe = clientTo(zks) flatMap { zkc => zkc.childrenOf(node1) }
           val modify =
             sleep1s ++
             clientTo(zks) flatMap { zkc =>
               eval_(zkc.create(node1, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
                 sleep1s ++ eval_(zkc.create(nodeA, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
                 sleep1s ++ eval_(zkc.create(nodeB, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
                 sleep1s ++ eval_(zkc.create(nodeC, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
                 sleep1s ++ eval_(zkc.delete(nodeB, None)) ++
                 sleep1s ++ eval_(zkc.delete(nodeC, None)) ++
                 sleep1s ++ eval_(zkc.delete(nodeA, None)) ++
                 sleep1s ++ eval_(zkc.delete(node1, None))
             }

           observe concurrently modify
         }
         . map { _.map(_._1) }
         .take(9).compile.toVector.unsafeRunTimed(10.seconds)

      result shouldBe Some(Vector(
        None
        , Some(List.empty)
        , Some(List(nodeA))
        , Some(List(nodeA, nodeB))
        , Some(List(nodeA, nodeB, nodeC))
        , Some(List(nodeA, nodeC))
        , Some(List(nodeA))
        , Some(List.empty)
        , None
      ))

    }


    "Signals correctly state of the client" in {
      val result =
        standaloneServer.flatMap { zks =>
          val observe = clientTo(zks) flatMap { _.clientState }
          val shutdown = Sch.sleep[IO](2.seconds) ++ eval_(zks.shutdown)
          val startup = Sch.sleep[IO](1.seconds) ++ eval_(zks.startup)
          observe concurrently (shutdown ++ startup)
        }
        .take(3)
        .compile.toVector.unsafeRunTimed(10.seconds)

      result shouldBe Some(Vector(
        ZkClientState.SyncConnected
        , ZkClientState.Disconnected
        , ZkClientState.SyncConnected
      ))
    }




  }


}
