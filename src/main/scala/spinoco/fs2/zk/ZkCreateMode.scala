package spinoco.fs2.zk


object ZkCreateMode {

  val Ephemeral = ZkCreateMode(ephemeral = true, sequential = false)
  val EphemeralSequential = ZkCreateMode(ephemeral = true, sequential = true)
  val Persistent = ZkCreateMode(ephemeral = false, sequential = false)
  val PersistentSequential = ZkCreateMode(ephemeral = false, sequential = true)


}



case class  ZkCreateMode(ephemeral:Boolean, sequential:Boolean)
