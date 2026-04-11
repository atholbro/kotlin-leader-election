package net.aholbrook.kotlinleaderelection

import java.io.File
import java.net.InetAddress

fun getHostname(): String = System.getenv("HOSTNAME") ?: InetAddress.getLocalHost().hostName

fun getNamespace(): String = runCatching {
    File("/var/run/secrets/kubernetes.io/serviceaccount/namespace").readText().trim()
}.getOrElse { error("Unable to get namespace from /var/run/secrets/kubernetes.io/serviceaccount/namespace") }