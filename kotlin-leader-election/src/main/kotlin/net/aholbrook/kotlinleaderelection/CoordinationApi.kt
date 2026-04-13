package net.aholbrook.kotlinleaderelection

import io.kubernetes.client.openapi.apis.CoordinationV1Api
import io.kubernetes.client.openapi.models.V1Lease
import io.kubernetes.client.openapi.models.V1LeaseSpec
import io.kubernetes.client.openapi.models.V1ObjectMeta
import java.time.ZoneOffset
import kotlin.time.Clock
import kotlin.time.toJavaInstant

internal interface CoordinationApi {
    fun readNamespacedLease(leaseName: String, namespace: String): V1Lease
    fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease
    fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease
}

internal class DelegatingCoordinationApi(private val api: CoordinationV1Api) : CoordinationApi {
    override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease =
        api.readNamespacedLease(leaseName, namespace).execute()

    override fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease =
        api.createNamespacedLease(namespace, lease).execute()

    override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease =
        api.replaceNamespacedLease(leaseName, namespace, lease).execute()
}

internal fun buildLeaseResource(
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

internal fun V1Lease.buildAcquire(identity: String, leaseDurationSeconds: Int, clock: Clock): V1Lease {
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

internal fun V1Lease.buildRelease(clock: Clock): V1Lease {
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
