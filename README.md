# Kotlin Leader Election (via Kubernetes Leases)
[![Build](https://github.com/atholbro/kotlin-leader-election/actions/workflows/build.yml/badge.svg)](https://github.com/atholbro/kotlin-leader-election/actions/workflows/build.yml)
[![Maven Central Version](https://img.shields.io/maven-central/v/net.aholbrook.kotlin-leader-election/kotlin-leader-election)](https://central.sonatype.com/artifact/net.aholbrook.kotlin-leader-election/kotlin-leader-election)
[![codecov](https://codecov.io/gh/atholbro/kotlin-leader-election/graph/badge.svg?token=LCJIWJH2EG)](https://codecov.io/gh/atholbro/kotlin-leader-election)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`kotlin-leader-election` is a small Kotlin library that coordinates leadership across replicas using
Kubernetes Lease objects (`coordination.k8s.io/v1`).

Typical use case: run one active instance of a background job (scheduler, poller, compactor, etc.)
while other replicas stay passive.

## Table of Contents
- [Requirements](#requirements)
  - [Kubernetes RBAC](#kubernetes-rbac)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Example](#basic-example)
  - [Role and Error Callbacks](#role-and-error-callbacks)
  - [Leader / Follower Loops](#leader--follower-loops)
  - [Default Identity and Namespace](#default-identity-and-namespace)
  - [Fully Explicit Configuration](#fully-explicit-configuration)

## Requirements

- JDK 17+
- Kubernetes cluster with Lease support (`coordination.k8s.io/v1`)

### Kubernetes RBAC
Your workload's `ServiceAccount` needs permissions to read and update Lease resources in its namespace.

At minimum, grant:
- `apiGroups: ["coordination.k8s.io"]`
- `resources: ["leases"]`
- `verbs: ["get", "create", "update"]`

For Helm charts, this is a typical namespaced `Role` + `RoleBinding`:

Create `templates/rbac.yaml` with:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "chart.fullname" . }}-leader-election
  namespace: {{ .Release.Namespace }}
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "chart.fullname" . }}-leader-election
  namespace: {{ .Release.Namespace }}
subjects:
  - kind: ServiceAccount
    name: {{ include "chart.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: Role
  name: {{ include "chart.fullname" . }}-leader-election
  apiGroup: rbac.authorization.k8s.io
```

If these permissions are missing, leader election calls will fail with Kubernetes API authorization errors.


## Installation
Add `net.aholbrook.kotlin-leader-election:kotlin-leader-election:0.1.0` to your dependencies.

```gradle
dependencies {
    implementation("net.aholbrook.kotlin-leader-election:kotlin-leader-election:0.1.0")
}
```

## Usage

### Basic Example
This example prints `"I am the leader"` every 15 seconds from exactly one replica at a time.

```kotlin
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.util.Config
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import net.aholbrook.kotlinleaderelection.LeaderElector
import kotlin.time.Duration.Companion.seconds

suspend fun main() = coroutineScope {
    val client: ApiClient = Config.defaultClient()
    Configuration.setDefaultApiClient(client)

    val elector = LeaderElector(leaseName = "test") {
        while (currentCoroutineContext().isActive) {
            println("I am the leader")
            delay(15.seconds)
        }
    }

    elector.start()

    try {
        awaitCancellation()
    } finally {
        elector.stop()
    }
}
```

### Start & Stop
Call `start()` to begin election and `stop()` for a graceful shutdown.

You can call `start()` and `stop()` multiple times, additional calls are ignored and the elector supports restarts.

When an elector is stopped, if it's the leader it will attempt to release the lease immediately.

### Role and Error Callbacks
You can react to role changes and errors with `onLeader`, `onFollower`, and `onError`.

- `onLeader`: called when this instance becomes leader
- `onFollower`: called when this instance transitions to follower
- `onError`: called for elector loop errors as `(cause, isFatal)`
  - `isFatal = false`: elector keeps running and will retry
  - `isFatal = true`: elector loop exits

```kotlin
import net.aholbrook.kotlinleaderelection.LeaderElector

val elector = LeaderElector(
    leaseName = "jobs",
    onError = { cause, isFatal ->
        println("leader election error (fatal=$isFatal): ${cause.message}")
    },
    onFollower = {
        println("Now follower")
    },
) {
    println("Now leader")
}
```

### Leader / Follower Loops
`onLeader` and `onFollower` are one-time transition callbacks. They are invoked when a role transition happens. When
roles switch, the library cancels the currently running role callback coroutine and starts the new role callback.
Work callbacks should be cooperative and cancellation-friendly.

In most applications you want long-running work while leader (and sometimes while follower), so you
typically run your own loop and check `currentCoroutineContext().isActive`.

```kotlin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import net.aholbrook.kotlinleaderelection.LeaderElector
import kotlin.time.Duration.Companion.seconds

val elector = LeaderElector(
    leaseName = "jobs",
    onFollower = {
        while (currentCoroutineContext().isActive) {
            println("following")
            delay(120.seconds)
        }
    },
) {
    while (currentCoroutineContext().isActive) {
        println("leading")
        delay(15.seconds)
    }
}
```

### Default Identity and Namespace
If you use the simple constructor, the library resolves these values automatically:

- `identity`: `HOSTNAME` env var, then local host name fallback
- `namespace`: reads from `/var/run/secrets/kubernetes.io/serviceaccount/namespace`

This is a good default for in-cluster workloads.

### Fully Explicit Configuration
Use the explicit constructor if you want to override namespace/identity and tuning values.

```kotlin
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.CoordinationV1Api
import io.kubernetes.client.util.Config
import net.aholbrook.kotlinleaderelection.LeaderElector
import kotlin.time.Duration.Companion.seconds

val client: ApiClient = Config.defaultClient()
val api = CoordinationV1Api(client)

val elector = LeaderElector(
    leaseName = "jobs",
    namespace = "platform",
    identity = "worker-a",
    api = api,
    leaseDuration = 30.seconds,
    renewalDelay = 5.seconds,
    onError = { cause, isFatal ->
        println("elector error (fatal=$isFatal): ${cause.message}")
    },
    onFollower = {
        stopLeaderOnlyWork()
    },
) {
    startLeaderOnlyWork()
}
```
