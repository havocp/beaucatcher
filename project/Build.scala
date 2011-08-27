import sbt._
import Keys._

object BuildSettings {
    val buildOrganization = "Havoc Pennington"
    val buildVersion = "0.1"
    val buildScalaVersion = "2.9.0-1"

    val globalSettings = Seq(
        organization := buildOrganization,
        version := buildVersion,
        scalaVersion := buildScalaVersion,
        shellPrompt := ShellPrompt.buildShellPrompt,
        fork in test := true,
        resolvers := Seq(Resolvers.scalaToolsSnapshotsRepo, Resolvers.akkaRepo, Resolvers.twttrRepo))

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
    val akkaRepo = "Akka Repo" at "http://akka.io/repository"
    val scalaToolsSnapshotsRepo = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"
    val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"
}

object Dependencies {
    val scalajCollection = "org.scalaj" %% "scalaj-collection" % "1.1"
    val scalap = "org.scala-lang" % "scalap" % "2.9.0-1"
    val commonsCodec = "commons-codec" % "commons-codec" % "1.4"
    val casbahCore = "com.mongodb.casbah" %% "casbah-core" % "2.1.5-1"
    val akkaActor = "se.scalablesolutions.akka" % "akka-actor" % "1.1"
    val hammersmithLib = "com.mongodb.async" %% "mongo-driver" % "0.2.7"

    // Dependencies in "test" configuration
    object Test {
        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test"
        val liftJson = "net.liftweb" %% "lift-json" % "2.4-SNAPSHOT" % "test"
    }
}

object BeaucatcherBuild extends Build {
    import BuildSettings._
    import Dependencies._
    import Resolvers._

    override lazy val settings = super.settings ++ globalSettings

    lazy val root = Project("beaucatcher",
        file("."),
        settings = projectSettings) aggregate (bson, mongo, casbah, async, hammersmith)

    lazy val bson = Project("beaucatcher-bson",
        file("bson"),
        settings = projectSettings ++
            Seq(checksums := Nil, // lift-json sha1 is hosed at the moment
                libraryDependencies := Seq(scalajCollection, scalap, commonsCodec, casbahCore,
                    Test.junitInterface, Test.liftJson)))

    lazy val mongo = Project("beaucatcher-mongo",
        file("mongo"),
        settings = projectSettings) dependsOn (bson % "compile->compile;test->test")

    lazy val casbah = Project("beaucatcher-casbah",
        file("casbah"),
        settings = projectSettings) dependsOn (mongo % "compile->compile;test->test")

    lazy val async = Project("beaucatcher-async",
        file("async"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(akkaActor))) dependsOn (mongo % "compile->compile;test->test")

    lazy val hammersmith = Project("beaucatcher-hammersmith",
        file("hammersmith"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(hammersmithLib))) dependsOn (async % "compile->compile;test->test")

    lazy val benchmark = Project("beaucatcher-benchmark",
        file("benchmark"),
        settings = projectSettings ++
            Seq(fork in run := true, javaOptions in run := Seq("-Xmx2G"))) dependsOn (hammersmith, casbah)
}
