package net.aholbrook.kotlinleaderelection

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.kubernetes.client.openapi.apis.CoordinationV1Api
import io.kubernetes.client.openapi.models.V1Lease
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Instant

class CoordinationApiTest {
    @Test
    fun `buildLeaseResource clamps seconds to min 1`() {
        val lease = buildLeaseResource(
            leaseName = "leaseName",
            namespace = "namespace",
            identity = "identity",
            leaseDurationSeconds = 0,
            resourceVersion = "1.0.0",
            clock = Clock.System,
        )

        lease.spec!!.leaseDurationSeconds shouldBe 1
    }

    @Test
    fun `buildLeaseResource sets values correctly`() {
        val clock = object : Clock {
            override fun now() = Instant.fromEpochSeconds(0)
        }

        val lease = buildLeaseResource(
            leaseName = "leaseName",
            namespace = "namespace",
            identity = "identity",
            leaseDurationSeconds = 1,
            resourceVersion = "1.0.0",
            clock = clock,
        )

        lease.metadata!!.name shouldBe "leaseName"
        lease.metadata!!.namespace shouldBe "namespace"
        lease.metadata!!.resourceVersion shouldBe "1.0.0"
        lease.spec!!.holderIdentity shouldBe "identity"
        lease.spec!!.leaseDurationSeconds shouldBe 1
        lease.spec!!.renewTime!!.toEpochSecond() shouldBe 0
    }

    @Test
    fun `buildAcquire null checks metadata`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().buildAcquire("", 1, Clock.System)
        }
    }

    @Test
    fun `buildAcquire null checks metadata name`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().metadata(V1ObjectMeta().namespace("ns1").resourceVersion("1"))
                .buildAcquire("", 1, Clock.System)
        }
    }

    @Test
    fun `buildAcquire null checks metadata namespace`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().metadata(V1ObjectMeta().name("test").resourceVersion("1"))
                .buildAcquire("", 1, Clock.System)
        }
    }

    @Test
    fun `buildAcquire null checks metadata resourceVersion`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().metadata(V1ObjectMeta().name("test").namespace("ns1"))
                .buildAcquire("", 1, Clock.System)
        }
    }

    @Test
    fun `buildRelease null checks metadata`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().buildRelease(Clock.System)
        }
    }

    @Test
    fun `buildRelease null checks metadata name`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().metadata(V1ObjectMeta().namespace("ns1").resourceVersion("1"))
                .buildRelease(Clock.System)
        }
    }

    @Test
    fun `buildRelease null checks metadata namespace`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().metadata(V1ObjectMeta().name("test").resourceVersion("1"))
                .buildRelease(Clock.System)
        }
    }

    @Test
    fun `buildRelease null checks metadata resourceVersion`() {
        shouldThrow<IllegalArgumentException> {
            V1Lease().metadata(V1ObjectMeta().name("test").namespace("ns1"))
                .buildRelease(Clock.System)
        }
    }

    @Nested
    @DisplayName("DelegatingCoordinationApi")
    inner class DelegatingCoordinationApiTest {
        @Test
        fun `read is forwarded to api`() {
            val api = mockk<CoordinationV1Api>()
            val call = mockk<CoordinationV1Api.APIreadNamespacedLeaseRequest>()
            val response = mockk<V1Lease>()

            every {
                api.readNamespacedLease("lease", "namespace")
            } returns call
            every { call.execute() } answers { response }

            DelegatingCoordinationApi(api)
                .readNamespacedLease("lease", "namespace") shouldBeSameInstanceAs response

            verify(exactly = 1) { api.readNamespacedLease("lease", "namespace") }
        }

        @Test
        fun `create is forwarded to api`() {
            val api = mockk<CoordinationV1Api>()
            val call = mockk<CoordinationV1Api.APIcreateNamespacedLeaseRequest>()
            val response = mockk<V1Lease>()

            every {
                api.createNamespacedLease("ns5", any())
            } returns call
            every { call.execute() } answers { response }

            val request = mockk<V1Lease>()
            DelegatingCoordinationApi(api)
                .createNamespacedLease("ns5", request) shouldBeSameInstanceAs response

            verify(exactly = 1) { api.createNamespacedLease("ns5", request) }
        }

        @Test
        fun `replace is forwarded to api`() {
            val api = mockk<CoordinationV1Api>()
            val call = mockk<CoordinationV1Api.APIreplaceNamespacedLeaseRequest>()
            val response = mockk<V1Lease>()

            every {
                api.replaceNamespacedLease("toReplace", "ns3", any())
            } returns call
            every { call.execute() } answers { response }

            val request = mockk<V1Lease>()
            DelegatingCoordinationApi(api)
                .replaceNamespacedLease("toReplace", "ns3", request) shouldBeSameInstanceAs response

            verify(exactly = 1) { api.replaceNamespacedLease("toReplace", "ns3", request) }
        }
    }
}
