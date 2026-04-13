package net.aholbrook.kotlinleaderelection

import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.CoordinationV1Api
import io.kubernetes.client.openapi.models.V1Lease
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

/**
 * Coordinates Kubernetes lease-based leader election.
 *
 * The elector runs a background coroutine that periodically reads or updates the target Lease.
 * Role changes trigger [onLeader] and [onFollower]. Errors are reported through [onError].
 *
 * Call [start] to begin participating in election and [stop] to stop participating.
 */
class LeaderElector internal constructor(
    private val leaseName: String,
    private val namespace: String,
    private val identity: String,
    private val api: CoordinationApi,
    private val leaseDuration: Duration,
    private val renewalDelay: Duration,
    private val clock: Clock,
    private val onError: (cause: Throwable, isFatal: Boolean) -> Unit,
    private val onFollower: (suspend () -> Unit),
    private val onLeader: suspend () -> Unit,
) {
    private val lifecycleMutex = Mutex()
    private val electorScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile
    private var role: Role = Role.STOPPED
    private var electorJob: Job = Job().apply { complete() }

    /**
     * Creates an elector using namespace and identity discovered from the runtime environment.
     *
     * @param leaseName lease name used for election coordination
     * @param api Kubernetes Coordination API client
     * @param leaseDuration desired lease duration used when creating or acquiring the lease
     * @param renewalDelay delay between election loop iterations
     * @param onError callback for elector loop errors, with `isFatal=true` when the loop exits
     * @param onFollower callback invoked when this elector transitions to follower
     * @param onLeader callback invoked when this elector transitions to leader
     */
    constructor(
        leaseName: String,
        api: CoordinationV1Api = CoordinationV1Api(),
        leaseDuration: Duration = 30.seconds,
        renewalDelay: Duration = 5.seconds,
        onError: (cause: Throwable, isFatal: Boolean) -> Unit = { _, _ -> },
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
        onError = onError,
        onFollower = onFollower,
        onLeader = onLeader,
    )

    /**
     * Creates an elector with explicit namespace and identity values.
     *
     * @param leaseName lease name used for election coordination
     * @param namespace Kubernetes namespace where the Lease exists
     * @param identity leaseholder identity written to the Lease when leader
     * @param api Kubernetes Coordination API client
     * @param leaseDuration desired lease duration used when creating or acquiring the lease
     * @param renewalDelay delay between election loop iterations
     * @param onError callback for elector loop errors, with `isFatal=true` when the loop exits
     * @param onFollower callback invoked when this elector transitions to follower
     * @param onLeader callback invoked when this elector transitions to leader
     */
    constructor(
        leaseName: String,
        namespace: String = getNamespace(),
        identity: String = getHostname(),
        api: CoordinationV1Api = CoordinationV1Api(),
        leaseDuration: Duration = 30.seconds,
        renewalDelay: Duration = 5.seconds,
        onError: (cause: Throwable, isFatal: Boolean) -> Unit = { _, _ -> },
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
        onError = onError,
        onFollower = onFollower,
        onLeader = onLeader,
    )

    /**
     * Starts participating in leader election.
     *
     * This function returns after launching the elector loop. It is safe to call multiple times;
     * subsequent calls while already running are no-ops.
     *
     * @param dispatcher dispatcher used to execute [onLeader] and [onFollower]
     */
    suspend fun start(dispatcher: CoroutineDispatcher = Dispatchers.Default) {
        lifecycleMutex.withLock {
            if (electorJob.isActive) return@withLock

            electorJob = electorScope.launch(CoroutineName("$leaseName-$identity-elector")) {
                electorLoop(dispatcher)
            }
        }
    }

    /**
     * Stops participating in leader election and waits for background work to finish.
     *
     * If this elector is currently leader, it attempts to release the lease before stopping.
     */
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
                        onError(e, true)
                        logger.error(e) { "Kubernetes API: auth error" }
                        break
                    }

                    500, 502, 503, 504 -> {
                        onError(e, false)
                        logger.warn(e) { "Transient API error ${e.code}, will retry" }
                    }

                    else -> {
                        onError(e, false)
                        logger.error(e) { "Unexpected Kubernetes API error" }
                    }
                }
            } catch (e: Throwable) {
                onError(e, true)
                logger.error(e) { "Unexpected error" }
                break
            }

            delay(renewalDelay)
        }

        runCatching {
            if (role == Role.LEADER) {
                releaseLeaderRole()
            }
        }
        role = Role.STOPPED
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

        val spec = lease.spec ?: error("Invalid lease, missing spec")
        val leader = spec.holderIdentity
        val renewTime = spec.renewTime
        val leaseDurationSeconds = requireNotNull(spec.leaseDurationSeconds) {
            "Invalid lease, missing leaseDurationSeconds"
        }
        require(leaseDurationSeconds > 0) { "Invalid lease, leaseDurationSeconds must be positive" }
        val now = clock.now().toJavaInstant().atOffset(ZoneOffset.UTC)
        val expired = renewTime == null || now.isAfter(renewTime.plusSeconds(leaseDurationSeconds.toLong()))

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

        val spec = lease.spec ?: error("Invalid lease, missing spec")
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
