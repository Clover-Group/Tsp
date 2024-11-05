ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always,
  "org.scala-lang.modules" %% "scala-parser-combinators" % VersionScheme.Always,
)
//lazy val root = project.in(file(".")).dependsOn(ghReleasePlugin)
//lazy val ghReleasePlugin = RootProject(uri("https://github.com/hyst329/sbt-github-release.git"))
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.1.0")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.4.0")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.2.2")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.2.3")