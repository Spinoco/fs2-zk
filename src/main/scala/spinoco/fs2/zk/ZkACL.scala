package spinoco.fs2.zk


/**
  * ACL Entry of ZooKeeper Node
  */
case class ZkACL (
  permission: ZkACL.Permission
  , scheme: String
  , entity: String
)


object ZkACL {

  val NoAccess = Permission(0)
  val Read = Permission(1)
  val Write = Permission(2)
  val Create = Permission(4)
  val Delete = Permission(8)
  val Admin =  Permission(16)

  case class Permission(value:Int)

  object Permission {
    implicit class  PermissionSyntax(val self:Permission ) extends AnyVal {

      def add(permission:Permission):Permission =
        Permission(self.value | permission.value)

      def remove(permission: Permission):Permission =
        Permission(self.value & (permission.value ^ 0xffffffff ) )
    }
  }




}