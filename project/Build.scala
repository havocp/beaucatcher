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
    val jodaTime = "joda-time" % "joda-time" % "1.6.2"

    // Dependencies in "test" configuration
    object Test {
        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test"
        val liftJson = "net.liftweb" %% "lift-json" % "2.4" % "test"
        val slf4j = "org.slf4j" % "slf4j-api" % "1.6.0"
        val mongoJavaDriver = Dependencies.mongoJavaDriver % "test"
        val commonsIO = "commons-io" % "commons-io" % "2.1" % "test"
    }
}

object BeaucatcherBuild extends Build {
    import BuildSettings._
    import Dependencies._
    import Resolvers._

    override lazy val settings = super.settings ++ globalSettings

    lazy val root = Project("beaucatcher",
        file("."),
        settings = projectSettings ++
            Seq(publishArtifact := false)) aggregate (bson, bsonJava, mongo, jdriver)

    lazy val bson = Project("beaucatcher-bson",
        file("bson"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(scalap, commonsCodec, jodaTime,
                    Test.junitInterface, Test.liftJson, Test.slf4j, Test.mongoJavaDriver)))

    lazy val mongo = Project("beaucatcher-mongo",
        file("mongo"),
        settings = projectSettings ++
              Seq(libraryDependencies ++= Seq(akkaActor))) dependsOn (bson % "compile->compile;test->test")

    // non-mongo aspects of mongo-java-driver
    lazy val bsonJava = Project("beaucatcher-bson-java",
        file("bson-java"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(mongoJavaDriver))) dependsOn (bson % "compile->compile;test->test")

    // backend for beaucatcher-mongo based on mongo-java-driver
    lazy val jdriver = Project("beaucatcher-java-driver",
        file("jdriver"),
        settings = projectSettings ++
              Seq(libraryDependencies ++= Seq(Test.commonsIO))) dependsOn (bsonJava, mongo % "compile->compile;test->test")
}
