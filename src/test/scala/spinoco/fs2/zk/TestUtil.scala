package spinoco.fs2.zk

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

import fs2.Async

/**
  * Created by pach on 14/05/16.
  */
object TestUtil {

  def removeRecursively[F[_]](path:Path)(implicit F:Async[F]):F[Unit] = {
    F.suspend {
      Files.walkFileTree(path, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
      ()
    }
  }

}
