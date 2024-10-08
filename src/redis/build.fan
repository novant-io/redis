#! /usr/bin/env fan

using build

class Build : build::BuildPod
{
  new make()
  {
    podName = "redis"
    summary = "Redis API for Fantom"
    version = Version("0.6")
    meta = [
      "org.name":     "Novant",
      "org.uri":      "https://novant.io/",
      "license.name": "MIT",
      "vcs.name":     "Git",
      "vcs.uri":      "https://github.com/novant-io/redis",
      "repo.public":  "true",
    ]
    depends = ["sys 1.0", "concurrent 1.0", "inet 1.0"]
    srcDirs = [`fan/`, `test/`]
    resDirs = [`doc/`]
    docApi  = true
    docSrc  = true
  }
}
