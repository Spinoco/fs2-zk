package spinoco.fs2.zk


object ZkOpResult {

  case class CreateResult(path:String) extends ZkOpResult
  case object DeleteResult extends ZkOpResult
  case class SetDataResult(stat:ZkStat) extends ZkOpResult
  case object CheckResult extends ZkOpResult
  case class ErrorResult(errCode:Int) extends ZkOpResult

}


sealed trait  ZkOpResult
