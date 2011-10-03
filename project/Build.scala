import sbt._
import Keys._

object BuildSettings {
    val buildOrganization = "org.beaucatcher"
    val buildVersion = "0.3.1"
    val buildScalaVersion = "2.9.0-1"

    val globalSettings = Seq(
        organization := buildOrganization,
        version := buildVersion,
        scalaVersion := buildScalaVersion,
        shellPrompt := ShellPrompt.buildShellPrompt,
        fork in test := true,
        publishTo in Scope.GlobalScope <<= (thisProjectRef) { (ref) =>
            val baseDir = new File(ref.build)
            Some(Resolver.file("gh-pages", new File(baseDir, "../beaucatcher-web/repository")))
        },
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
    val scalap = "org.scala-lang" % "scalap" % "2.9.0-1"
    val commonsCodec = "commons-codec" % "commons-codec" % "1.4"
    val mongoJavaDriver  = "org.mongodb" % "mongo-java-driver" % "2.6.5"
    val casbahCore = "com.mongodb.casbah" %% "casbah-core" % "2.1.5-1"
    val akkaActor = "se.scalablesolutions.akka" % "akka-actor" % "1.1"
    val hammersmithLib = "com.mongodb.async" %% "mongo-driver" % "0.2.7"
    val jodaTime = "joda-time" % "joda-time" % "1.6.2"

    // Dependencies in "test" configuration
    object Test {
        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test"
        val liftJson = "net.liftweb" %% "lift-json" % "2.4-SNAPSHOT" % "test"
        val slf4j = "org.slf4j" % "slf4j-api" % "1.6.0"
        val mongoJavaDriver = Dependencies.mongoJavaDriver % "test"
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
            Seq(publishArtifact := false)) aggregate (bson, bsonJava, mongo, async, casbah)

    lazy val bson = Project("beaucatcher-bson",
        file("bson"),
        settings = projectSettings ++
            Seq(checksums := Nil, // lift-json sha1 is hosed at the moment
                libraryDependencies := Seq(scalap, commonsCodec, jodaTime,
                    Test.junitInterface, Test.liftJson, Test.slf4j, Test.mongoJavaDriver)))

    lazy val bsonJava = Project("beaucatcher-bson-java",
        file("bson-java"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(mongoJavaDriver))) dependsOn (bson % "compile->compile;test->test")

    lazy val mongo = Project("beaucatcher-mongo",
        file("mongo"),
        settings = projectSettings) dependsOn (bson % "compile->compile;test->test")

    lazy val casbah = Project("beaucatcher-casbah",
        file("casbah"),
        settings = projectSettings ++
            makeGenerateBsonJavaSettings("org.beaucatcher.casbah") ++
            Seq(libraryDependencies += casbahCore)) dependsOn (mongo % "compile->compile;test->test")

    lazy val async = Project("beaucatcher-async",
        file("async"),
        settings = projectSettings ++
            Seq(libraryDependencies := Seq(akkaActor))) dependsOn (mongo % "compile->compile;test->test")

    lazy val hammersmith = Project("beaucatcher-hammersmith",
        file("hammersmith"),
        settings = projectSettings ++
            makeGenerateBsonJavaSettings("org.beaucatcher.hammersmith") ++
            Seq(libraryDependencies := Seq(hammersmithLib))) dependsOn (async % "compile->compile;test->test")

    lazy val benchmark = Project("beaucatcher-benchmark",
        file("benchmark"),
        settings = projectSettings ++
            Seq(fork in run := true, javaOptions in run := Seq("-Xmx2G"))) dependsOn (hammersmith, casbah)

    def makeGenerateBsonJavaSettings(packageName: String) = {
        Seq(
            generateBsonJava <<= (streams, scalaSource in Compile) map {
                (streams, srcDir) =>
                    generateBsonJavaTask(streams, srcDir, packageName)
            },
            sources in Compile <<= (generateBsonJava, sources in Compile) map {
                (generated, sources) =>
                    (sources ++ generated).distinct
            })
    }

    // This is wonky; the issue is that hammersmith and casbah each
    // pull in mongo-java-driver upstream, and hammersmith (for now) even
    // has part of mongo-java-driver internally as a lib/, so they may
    // not match the mongo-java-driver we download. Thus we don't want
    // to use the beaucatcher-bson-java jar, but instead we generate
    // source and recompile it against whatever comes with hammersmith
    // and casbah. Pretty weird hack, but no way to generate a shared
    // binary against two possibly ABI-incompatible mongo-java-driver
    // I guess
    val generateBsonJava = TaskKey[Seq[File]]("generate-bson-java", "Generate private copy of bson-java sources in hammersmith and casbah projects")

    def generateBsonJavaTask(streams: TaskStreams, srcDir: File, packageName: String) = {
        // there is probably a better way to do this
        val bsonJavaDir = srcDir /  "../../../../bson-java/src/main/scala"
        val bsonJavaSources = PathFinder(bsonJavaDir) ** new SimpleFileFilter({ f =>
            f.getName.endsWith(".scala")
        })

        val lastPackageComponent = packageName.substring(packageName.lastIndexOf('.') + 1)

        val generatedBuilder = Seq.newBuilder[File]
        val targetDir = srcDir / packageName.replaceAll("""\.""", "/") / "j"
        IO.createDirectory(targetDir)

        for (src <- bsonJavaSources.get) {
            val template = IO.read(src asFile)
            val target = targetDir / src.getName
            val newContent = template.
                replaceAll("""package org\.beaucatcher\.bson""",
                           "package " + packageName + ".j\n" +
                       "import org.beaucatcher.bson._\n").
                replaceAll("""object """, "private[" + lastPackageComponent + "] object ")
            val oldContent = try {
                IO.read(target asFile)
            } catch {
                case e: java.io.IOException => ""
            }
            if (newContent != oldContent) {
                streams.log.info("Generated " + target)
                IO.write(target asFile, newContent)
            }
            generatedBuilder += target
        }

        val pkgFile = targetDir / "package.scala"

        val pkgContent = """
package %s

package object j {
}
""".format(packageName, packageName)

        val oldPkgContent = try {
            IO.read(pkgFile asFile)
        } catch {
            case e: java.io.FileNotFoundException => ""
        }

        if (oldPkgContent != pkgContent) {
            IO.write(pkgFile asFile, pkgContent)
            streams.log.info("Generated " + pkgFile)
        }

        generatedBuilder += pkgFile

        generatedBuilder.result
    }
}
