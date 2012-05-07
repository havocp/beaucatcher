import sbt._
import Keys._

object BuildSettings {
    val buildOrganization = "org.beaucatcher"
    val buildVersion = "0.3.1-SNAPSHOT"
    val buildScalaVersion = "2.9.1"

    val globalSettings = Seq(
        organization := buildOrganization,
        version := buildVersion,
        scalaVersion := buildScalaVersion,
        shellPrompt := ShellPrompt.buildShellPrompt,
        fork in test := true,
        checksums := Nil, // lift-json sha1 is hosed at the moment
        publishTo in Scope.GlobalScope <<= (thisProjectRef) { (ref) =>
            val baseDir = new File(ref.build)
            Some(Resolver.file("gh-pages", new File(baseDir, "../beaucatcher-web/repository")))
        },
        resolvers := Seq(Resolvers.scalaToolsSnapshotsRepo, Resolvers.typesafeRepo, Resolvers.twttrRepo))

    val projectSettings = Defaults.defaultSettings ++ globalSettings
}

object ShellPrompt {
    object devnull extends ProcessLogger {
        def info(s : => String) {}
        def error(s : => String) {}
        def buffer[T](f : => T) : T = f
    }

    val current = """\*\s+([\w-\.]+)""".r

    def gitBranches = ("git branch --no-color" lines_! devnull mkString)

    val buildShellPrompt = { (state : State) =>
        {
            val currBranch =
                current findFirstMatchIn gitBranches map (_ group (1)) getOrElse "-"
            val currProject = Project.extract(state).currentProject.id
            "%s:%s:%s> ".format(currProject, currBranch, BuildSettings.buildVersion)
        }
    }
}

object Resolvers {
    val typesafeRepo = "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
    val scalaToolsSnapshotsRepo = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"
    val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"
}

object Dependencies {
    val scalap = "org.scala-lang" % "scalap" % BuildSettings.buildScalaVersion
    val commonsCodec = "commons-codec" % "commons-codec" % "1.4"
    val mongoJavaDriver  = "org.mongodb" % "mongo-java-driver" % "2.7.3"
    val akkaActor = "com.typesafe.akka" % "akka-actor" % "2.0"
    val netty = "io.netty" % "netty" % "3.3.1.Final"

    // Dependencies in "test" configuration
    object Test {
        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test"
        val liftJson = "net.liftweb" %% "lift-json" % "2.4" % "test"
        val slf4j = "org.slf4j" % "slf4j-api" % "1.6.4"
        val mongoJavaDriver = Dependencies.mongoJavaDriver % "test"
        val commonsIO = "commons-io" % "commons-io" % "2.1" % "test"
    }
}

object BeaucatcherBuild extends Build {
    import BuildSettings._
    import Dependencies._
    import Resolvers._

    override lazy val settings = super.settings ++ globalSettings

    // this is a weird syntax because the usual apply()
    // is limited to 9 items in sbt 0.11
    lazy val rootMembers: Seq[ProjectReference] =
        Seq(base, channel, channelNetty, bson,
            mongo, driver, jdriver,
            channelDriver, mongoTest).map({ p: Project => LocalProject(p.id) })

    lazy val root = Project("beaucatcher",
        file("."),
        aggregate = rootMembers,
        settings = projectSettings ++
            Seq(publishArtifact := false))

    // constants and other miscellany for the bson/mongo wire protocol
    lazy val base = Project("beaucatcher-base",
        file("base"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(Test.junitInterface, Test.slf4j, akkaActor)))

    // abstract API for a mongo channel
    lazy val channel = Project("beaucatcher-channel",
        file("channel"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(akkaActor))) dependsOn(base)

    // netty implementation of a mongo channel
    lazy val channelNetty = Project("beaucatcher-channel-netty",
        file("channel-netty"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(netty))) dependsOn(channel % "compile->compile;test->test", bson % "test->test")

    // bson/json parsing and syntax tree
    lazy val bson = Project("beaucatcher-bson",
        file("bson"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(scalap, commonsCodec,
                    Test.junitInterface, Test.liftJson, Test.slf4j, Test.mongoJavaDriver))) dependsOn(base % "compile->compile;test->test")

    // interface to driver
    lazy val driver = Project("beaucatcher-driver",
        file("driver"),
        settings = projectSettings ++
              Seq(libraryDependencies ++= Seq())) dependsOn (base % "compile->compile;test->test",
                                // FIXME don't depend on bson
                                bson % "compile->compile;test->test")

    // mongo API
    lazy val mongo = Project("beaucatcher-mongo",
        file("mongo"),
        settings = projectSettings ++
              Seq()) dependsOn (bson % "compile->compile;test->test", driver % "compile->compile;test->test")

    // backend for beaucatcher-mongo based on mongo-java-driver
    lazy val jdriver = Project("beaucatcher-java-driver",
        file("jdriver"),
        settings = projectSettings ++
              Seq(libraryDependencies ++= Seq(mongoJavaDriver, Test.commonsIO))) dependsOn (driver % "compile->compile;test->test")

    // backend for beaucatcher-mongo based on channels
    lazy val channelDriver = Project("beaucatcher-channel-driver",
        file("channel-driver"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq())) dependsOn(channelNetty % "compile->compile;test->test", driver % "compile->compile;test->test")

    // mongo tests at top of dependency chain
    lazy val mongoTest = Project("beaucatcher-mongo-test",
        file("mongo-test"),
        settings = projectSettings ++
              Seq()) dependsOn (mongo % "compile->compile;test->test", jdriver % "compile->compile;test->test", channelDriver % "compile->compile;test->test", bson % "compile->compile;test->test")
}
