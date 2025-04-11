package com.versiobit.objectstore.s3

import com.versiobit.objectstore.ObjectInfo
import com.versiobit.objectstore.ObjectStore
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*

class S3ObjectStore(private val bucket: String, private val s3: S3AsyncClient) : ObjectStore {

    private val log = LoggerFactory.getLogger(this.javaClass)

    override suspend fun doAnyObjectsExist(keyPrefix: String): Boolean {
        return s3.listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(keyPrefix)
                .build()
        ).await().keyCount() != 0
    }

    override suspend fun getObjectAsByteArrayOrNull(key: String): ByteArray? {
        if (log.isTraceEnabled) log.trace("Loading S3 object from $key")
        return try {
            return s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(),
                AsyncResponseTransformer.toBytes()
            ).await().asByteArray()
        } catch (e: NoSuchKeyException) {
            null
        }
    }

    override suspend fun headObject(key: String): ObjectInfo? {
        return try {
            val obj = s3.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            ).await()
            ObjectInfo(obj.contentLength())
        } catch (e: NoSuchKeyException) {
            if (log.isTraceEnabled) log.trace("HeadObject for s3://$bucket/$key failed with $e")
            null
        }
    }

    override suspend fun putObject(key: String, data: ByteArray) {
        if (log.isTraceEnabled) log.trace("Putting S3 object at $key")
        s3.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build(),
            AsyncRequestBody.fromBytes(data)
        ).await()
    }

    override suspend fun deleteObject(key: String) {
        if (log.isTraceEnabled) log.trace("Deleting S3 object at $key")
        s3.deleteObject(
            DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()
        ).await()
    }

    override suspend fun deleteObjects(keys: List<String>) {
        if (log.isTraceEnabled) log.trace("Deleting ${keys.size} S3 objects")
        val objectIds = keys.map { ObjectIdentifier.builder().key(it).build() }
        s3.deleteObjects(
            DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(
                    Delete.builder()
                        .objects(objectIds)
                        .build()
                )
                .build()
        ).await()
    }

    override suspend fun deleteRecursive(keyPrefix: String) {
        log.debug("Recursively deleting everything in S3 bucket '$bucket' under path: '$keyPrefix'")

        val listRequest = ListObjectVersionsRequest.builder()
            .bucket(bucket)
            .prefix(keyPrefix)
            .build()

        s3.listObjectVersionsPaginator(listRequest).map { page ->
            val objects = page.versions().map {
                ObjectIdentifier.builder().key(it.key()).versionId(it.versionId()).build()
            }

            if (objects.isNotEmpty()) {
                log.debug("Deleting a page of ${page.versions().size} objects")
                // TODO make this non-blocking, see copyRecursive
                s3.deleteObjects(
                    DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(
                            Delete.builder()
                                .objects(objects)
                                .build()
                        )
                        .build()
                ).get()
            } else {
                log.debug("No objects found")
                null
            }
        }

        log.debug("Recursive deletion completed")
    }

    override suspend fun copyObjectFrom(sourceObjectStore: ObjectStore, sourceKey: String, destinationKey: String) {
        if (sourceObjectStore is S3ObjectStore) {
            if (log.isTraceEnabled) log.trace("Copying s3://${sourceObjectStore.bucket}/$sourceKey to s3://$bucket/$destinationKey")
            s3.copyObject(
                CopyObjectRequest.builder()
                    .sourceBucket(sourceObjectStore.bucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucket)
                    .destinationKey(destinationKey)
                    .metadataDirective(MetadataDirective.REPLACE)
                    .storageClass(StorageClass.STANDARD)
                    .build()
            ).await()
        } else {
            super.copyObjectFrom(sourceObjectStore, sourceKey, destinationKey)
        }
    }

    override suspend fun copyRecursive(srcKeyPrefix: String, destKeyPrefix: String) {
        log.debug("Recursively copying everything in S3 bucket '$bucket' under path '$srcKeyPrefix' to '$destKeyPrefix'")

        suspend fun processPage(page: ListObjectsV2Response) {
            log.debug("Copying a page of ${page.contents().size} objects")
            page.contents().forEach { srcObject ->
                val destKey = srcObject.key().replaceFirst(srcKeyPrefix, destKeyPrefix)
                copyObjectFrom(this@S3ObjectStore, srcObject.key(), destKey)
            }
        }

        coroutineScope {
            val listRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(srcKeyPrefix)
                .build()

            s3.listObjectsV2Paginator(listRequest).subscribe { page ->
                this.launch {
                    processPage(page)
                }
            }.get()
        }

        log.debug("Recursive copy completed")
    }

}
