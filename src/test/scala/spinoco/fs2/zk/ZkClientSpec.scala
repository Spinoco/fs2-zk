package spinoco.fs2.zk

import fs2.Stream._
import fs2.{Chunk, time}

import concurrent.duration._

class ZkClientSpec extends Fs2ZkClientSpec {


  "ZooKeeper Client" - {

    val node1 = ZkNode.parse("/n1").get

    def sleep1s = time.sleep(1.second)

    "create and delete Node" in {

      val result =
        standaloneServerAndClient.flatMap { case (zks, zkc) =>
            eval(zkc.create(node1, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))).map(Right(_)) ++
            eval(zkc.existsNow(node1)).map(Left(_))
        }.runLog.run.unsafeRun

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
                  sleep1s ++ eval_(zkc.setDataOf(node1, Some(new Chunk.Bytes(Array[Byte](1,2,3), 0, 3)), None)) ++
                  sleep1s ++ eval_(zkc.delete(node1, None))
            }

          observe mergeDrainR modify
        }
        .map { _.map(_.dataLength) }
        .take(4).runLog.run.timed(5.seconds).unsafeRun

      result shouldBe Vector(
        None
        , Some(0)
        , Some(3)
        , None
      )

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

           observe mergeDrainR modify
         }
         . map { _.map(_._1) }
         .take(9).runLog.run.timed(10.seconds).unsafeRun

      result shouldBe Vector(
        None
        , Some(List.empty)
        , Some(List(nodeA))
        , Some(List(nodeA, nodeB))
        , Some(List(nodeA, nodeB, nodeC))
        , Some(List(nodeA, nodeC))
        , Some(List(nodeA))
        , Some(List.empty)
        , None
      )

    }


    "Signals correctly state of the client" in {
      val result =
        standaloneServer.flatMap { zks =>
          val observe = clientTo(zks) flatMap { _.clientState }
          val shutdown = time.sleep(2.seconds) ++ eval_(zks.shutdown)
          val startup = time.sleep(1.seconds) ++ eval_(zks.startup)
          observe mergeDrainR (shutdown ++ startup)
        }
        .take(3)
        .runLog.run.timed(10.seconds).unsafeRun

      result shouldBe Vector(
        ZkClientState.SyncConnected
        , ZkClientState.Disconnected
        , ZkClientState.SyncConnected
      )
    }




  }


}
