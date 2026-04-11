package net.aholbrook.kotlinleaderelection

import io.kubernetes.client.openapi.apis.CoordinationV1Api
import io.kubernetes.client.openapi.models.V1Lease

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
