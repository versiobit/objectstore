package com.versiobit.objectstore.s3

import com.versiobit.objectstore.ObjectStore
import com.versiobit.objectstore.ObjectStoreTests
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class S3ObjectStoreTest : ObjectStoreTests() {

    private val bucket = "versiobit-objectstore-test"

    @Test
    fun copyIgnoreMetadata() = runTest {
        val objectStore = createObjectStore()
        val s3 = createS3Client()

        // upload src
        val srcKey = "uploads/123.txt"
        val srcContent = "abc"
        s3.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(srcKey)
                .metadata(mapOf("eval" to "eval value"))
                .websiteRedirectLocation("https://google.de")
                .build(), AsyncRequestBody.fromString(srcContent)
        ).await()

        val destKey = "copy/123.txt"
        objectStore.copyObjectFrom(objectStore, srcKey, destKey)

        val blobObject = s3.headObject(
            HeadObjectRequest.builder()
                .bucket(bucket)
                .key(destKey)
                .build()
        ).await()

        assertEquals(3, blobObject.contentLength())
        assertNull(blobObject.metadata()["eval"])
        assertNull(blobObject.websiteRedirectLocation())
    }

    private fun createS3Client(): S3AsyncClient = S3AsyncClient.create()

    override fun createObjectStore(): ObjectStore {
        return S3ObjectStore(bucket, createS3Client())
    }

}
