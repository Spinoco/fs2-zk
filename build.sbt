
val ReleaseTag = """^release/([\d\.]+a?)$""".r

lazy val contributors = Seq(
  "pchlupacek" -> "Pavel Chlupáček"
)

lazy val commonSettings = Seq(
  organization := "com.spinoco",
  scalaVersion := "2.13.16",
  crossScalaVersions := Seq("2.12.20", "2.13.16"),
  scalacOptions ++= {
    val commonOptions = Seq(
      "-feature",
      "-deprecation",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials",
      "-language:postfixOps",
      "-Xfatal-warnings"
    )
    if (scalaVersion.value.startsWith("2.12")) {
      commonOptions ++ Seq(
        "-Yno-adapted-args",
        "-Ywarn-value-discard",
        "-Ywarn-unused-import"
      )
    } else {
      commonOptions ++ Seq(
        "-Wvalue-discard",
        "-Wunused:imports",
        "-Wconf:cat=deprecation&msg=JavaConverters:s"
      )
    }
  },
  Compile / console / scalacOptions ~= {opts => opts.filterNot(Set("-Ywarn-unused-import", "-Wunused:imports").contains)},
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.19" % "test"
    , "org.scalacheck" %% "scalacheck" % "1.18.1" % "test"
    , "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % "test"
    , "org.slf4j" % "slf4j-simple" % "1.6.1" % "test" // uncomment this for logs when testing
    , "co.fs2" %% "fs2-core" % "3.12.2"
    , "co.fs2" %% "fs2-io" % "3.12.2"
    , "org.apache.zookeeper" % "zookeeper" % "3.9.4"

  ),
  scmInfo := Some(ScmInfo(url("https://github.com/Spinoco/fs2-zk"), "git@github.com:Spinoco/fs2-zk.git")),
  homepage := None,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  initialCommands := s"""
    import fs2._
    import spinoco.fs2.zk._
  """
) ++ testSettings ++ scaladocSettings ++ publishingSettings ++ releaseSettings

lazy val testSettings = Seq(
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  Test / publishArtifact := true
)

lazy val scaladocSettings = Seq(
  Compile / doc / scalacOptions ++= Seq(
    "-doc-source-url", scmInfo.value.get.browseUrl + "/tree/master€{FILE_PATH}.scala",
    "-sourcepath", (LocalRootProject / baseDirectory).value.getAbsolutePath,
    "-implicits",
    "-implicits-show-all"
  ),
  Compile / doc / scalacOptions ~= { _ filterNot { _ == "-Xfatal-warnings" } },
  autoAPIMappings := true
)

lazy val publishingSettings = Seq(
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/Spinoco/fs2-zk</url>
    <developers>
      {for ((username, name) <- contributors) yield
      <developer>
        <id>{username}</id>
        <name>{name}</name>
        <url>http://github.com/{username}</url>
      </developer>
      }
    </developers>
  },
  pomPostProcess := { node =>
    import scala.xml._
    import scala.xml.transform._
    def stripIf(f: Node => Boolean) = new RewriteRule {
      override def transform(n: Node) =
        if (f(n)) NodeSeq.Empty else n
    }
    val stripTestScope = stripIf { n => n.label == "dependency" && (n \ "scope").text == "test" }
    new RuleTransformer(stripTestScope).transform(node)(0)
  }
)

lazy val releaseSettings = Seq(
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value
)

lazy val `fs2-zk` =
  project.in(file("."))
  .settings(commonSettings)
  .settings(
   name := "fs2-zk"
  ) 
 
 

