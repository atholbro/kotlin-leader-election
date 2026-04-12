package net.aholbrook.kotlinleaderelection

import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.CoordinationV1Api
import io.kubernetes.client.openapi.models.V1Lease
import io.kubernetes.client.openapi.models.V1LeaseSpec
import io.kubernetes.client.openapi.models.V1ObjectMeta
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import java.time.ZoneOffset
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaInstant

private val logger = KotlinLogging.logger {}

private enum class Role {
    LEADER,
    FOLLOWER,
    STOPPED,
}

class LeaderElector internal constructor(
    private val leaseName: String,
    private val namespace: String,
    private val identity: String,
    private val api: CoordinationApi,
    private val leaseDuration: Duration,
    private val renewalDelay: Duration,
    private val clock: Clock,
    private val onFollower: (suspend () -> Unit),
    private val onLeader: suspend () -> Unit,
) {
    private val lifecycleMutex = Mutex()
    private val electorScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile
    private var role: Role = Role.STOPPED
    private var electorJob: Job = Job().apply { complete() }

    constructor(
        leaseName: String,
        api: CoordinationV1Api = CoordinationV1Api(),
        leaseDuration: Duration = 30.seconds,
        renewalDelay: Duration = 5.seconds,
        onFollower: (suspend () -> Unit) = { },
        onLeader: suspend () -> Unit,
    ) : this(
        leaseName = leaseName,
        namespace = getNamespace(),
        identity = getHostname(),
        api = DelegatingCoordinationApi(api),
        leaseDuration = leaseDuration,
        renewalDelay = renewalDelay,
        clock = Clock.System,
        onFollower = onFollower,
        onLeader = onLeader,
    )

    constructor(
        leaseName: String,
        namespace: String = getNamespace(),
        identity: String = getHostname(),
        api: CoordinationV1Api = CoordinationV1Api(),
        leaseDuration: Duration = 30.seconds,
        renewalDelay: Duration = 5.seconds,
        onFollower: (suspend () -> Unit) = { },
        onLeader: suspend () -> Unit,
    ) : this(
        leaseName = leaseName,
        namespace = namespace,
        identity = identity,
        api = DelegatingCoordinationApi(api),
        leaseDuration = leaseDuration,
        renewalDelay = renewalDelay,
        clock = Clock.System,
        onFollower = onFollower,
        onLeader = onLeader,
    )

    suspend fun start(dispatcher: CoroutineDispatcher = Dispatchers.Default) {
        lifecycleMutex.withLock {
            if (electorJob.isActive) return@withLock

            electorJob = electorScope.launch(CoroutineName("$leaseName-$identity-elector")) {
                electorLoop(dispatcher)
            }
        }
    }

    suspend fun stop() {
        lifecycleMutex.withLock {
            electorJob.cancelAndJoin()

            if (role == Role.LEADER) {
                releaseLeaderRole()
            }
            role = Role.STOPPED
        }
    }

    private suspend fun CoroutineScope.electorLoop(dispatcher: CoroutineDispatcher) {
        var roleJob: Job = Job().apply { complete() }

        while (isActive) {
            try {
                val oldRole = role
                acquireLeaderRole()

                if (role != oldRole) {
                    roleJob.cancelAndJoin()

                    roleJob = when (role) {
                        Role.LEADER -> startLeader(dispatcher)
                        else -> startFollower(dispatcher)
                    }
                }
            } catch (e: ApiException) {
                when (e.code) {
                    401, 403 -> {
                        logger.error(e) { "Kubernetes API: auth error" }
                        break
                    }

                    500, 502, 503, 504 -> {
                        logger.warn(e) { "Transient API error ${e.code}, will retry" }
                    }

                    else -> {
                        logger.error(e) { "Unexpected Kubernetes API error" }
                    }
                }
            } catch (e: Throwable) {
                logger.error(e) { "Unexpected error" }
                break
            }

            delay(renewalDelay)
        }
    }

    private fun CoroutineScope.startLeader(dispatcher: CoroutineDispatcher) =
        launch(dispatcher + CoroutineName("$leaseName-$identity-leader")) {
            onLeader()
        }

    private fun CoroutineScope.startFollower(dispatcher: CoroutineDispatcher): Job =
        launch(dispatcher + CoroutineName("$leaseName-$identity-follower")) {
            onFollower()
        }

    private fun acquireLeaderRole() {
        val lease = try {
            api.readNamespacedLease(leaseName, namespace)
        } catch (e: ApiException) {
            when (e.code) {
                404 -> {
                    createLease()
                    return
                }

                else -> throw e
            }
        }

        val spec = lease.spec ?: return
        val leader = spec.holderIdentity
        val renewTime = spec.renewTime
        val leaseDurationSeconds = (spec.leaseDurationSeconds?.toLong() ?: leaseDuration.inWholeSeconds)
            .coerceAtLeast(1)
        val now = clock.now().toJavaInstant().atOffset(ZoneOffset.UTC)
        val expired = renewTime == null || now.isAfter(renewTime.plusSeconds(leaseDurationSeconds))

        when {
            leader == identity -> {
                logger.debug { "[$identity] renewing lease" }
                updateLease(lease.buildAcquire(identity, leaseDuration.inWholeSeconds.toInt(), clock))
            }

            expired -> {
                logger.info { "[$identity] lease expired, attempting to acquire leader role" }
                updateLease(lease.buildAcquire(identity, leaseDuration.inWholeSeconds.toInt(), clock))
            }

            else -> {
                if (role == Role.LEADER) {
                    logger.info { "[$identity] lost leader role" }
                }
                role = Role.FOLLOWER
            }
        }
    }

    private fun releaseLeaderRole() {
        val lease = try {
            api.readNamespacedLease(leaseName, namespace)
        } catch (e: ApiException) {
            when (e.code) {
                404, 409 -> return
                else -> throw e
            }
        }

        val spec = lease.spec ?: return
        val leader = spec.holderIdentity

        if (leader == identity) {
            updateLease(lease.buildRelease(clock))
        }
    }

    private fun createLease() {
        val lease = buildLeaseResource(
            leaseName = leaseName,
            namespace = namespace,
            identity = identity,
            leaseDurationSeconds = leaseDuration.inWholeSeconds.toInt(),
            resourceVersion = null,
            clock = clock,
        )

        try {
            api.createNamespacedLease(namespace, lease)
            logger.info { "Created lease and became leader" }
            role = Role.LEADER
        } catch (e: ApiException) {
            when (e.code) {
                409 -> role = Role.FOLLOWER
                else -> throw e
            }
        }
    }

    private fun updateLease(lease: V1Lease) {
        try {
            api.replaceNamespacedLease(leaseName, namespace, lease)
            role = if (lease.spec?.holderIdentity == identity) Role.LEADER else Role.FOLLOWER
        } catch (e: ApiException) {
            when (e.code) {
                404, 409 -> role = Role.FOLLOWER
                else -> throw e
            }
        }
    }
}

private fun buildLeaseResource(
    leaseName: String,
    namespace: String,
    identity: String?,
    leaseDurationSeconds: Int,
    resourceVersion: String?,
    clock: Clock,
): V1Lease = V1Lease()
    .metadata(
        V1ObjectMeta()
            .name(leaseName)
            .namespace(namespace)
            .resourceVersion(resourceVersion),
    ).spec(
        V1LeaseSpec()
            .holderIdentity(identity)
            .leaseDurationSeconds(leaseDurationSeconds.coerceAtLeast(1))
            .renewTime(clock.now().toJavaInstant().atOffset(ZoneOffset.UTC)),
    )

private fun V1Lease.buildAcquire(identity: String, leaseDurationSeconds: Int, clock: Clock): V1Lease {
    val metadata = requireNotNull(metadata)

    return buildLeaseResource(
        leaseName = requireNotNull(metadata.name),
        namespace = requireNotNull(metadata.namespace),
        identity = identity,
        leaseDurationSeconds = leaseDurationSeconds,
        resourceVersion = requireNotNull(metadata.resourceVersion),
        clock = clock,
    )
}

private fun V1Lease.buildRelease(clock: Clock): V1Lease {
    val metadata = requireNotNull(metadata)

    return buildLeaseResource(
        leaseName = requireNotNull(metadata.name),
        namespace = requireNotNull(metadata.namespace),
        identity = null,
        leaseDurationSeconds = 1,
        resourceVersion = requireNotNull(metadata.resourceVersion),
        clock = clock,
    )
}
