package spinoco.fs2.zk

import org.apache.zookeeper.ZooDefs


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
  val Read = Permission(ZooDefs.Perms.READ)
  val Write = Permission(ZooDefs.Perms.WRITE)
  val Create = Permission(ZooDefs.Perms.CREATE)
  val Delete = Permission(ZooDefs.Perms.DELETE)
  val Admin =  Permission(ZooDefs.Perms.ADMIN)
  val All =  Permission(ZooDefs.Perms.ALL)

  val ACL_OPEN_UNSAFE = ZkACL(All,"world","anyone")


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