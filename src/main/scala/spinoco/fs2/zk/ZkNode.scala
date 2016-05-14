package spinoco.fs2.zk

import org.apache.zookeeper.common.PathUtils

import scala.util.Try

/**
  * Validated name of ZkNode.
  */
case class ZkNode protected[zk](path:String)


object ZkNode {


  /**
    * Constructs ZkNode by parsing supplied string. Will return a failure, if the path cannot be parsed
    * to valid ZkNode path
    */
  def parse(path:String):Try[ZkNode] =
    Try(PathUtils.validatePath(path)).map(_ => ZkNode(path))



  implicit class ZkNodeSyntax(val self: ZkNode) extends AnyVal {
    def / (path:String) :Try[ZkNode] = parse(self.path+"/"+path)
  }


}