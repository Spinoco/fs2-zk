package spinoco.fs2.zk

import scala.jdk.CollectionConverters._

private[zk] object ListConverters {
  def toScalaList[T](list: java.util.List[T]): List[T] =
    list.asScala.toList

  def toJavaList[T](list: List[T]): java.util.List[T] =
    list.asJava
}
