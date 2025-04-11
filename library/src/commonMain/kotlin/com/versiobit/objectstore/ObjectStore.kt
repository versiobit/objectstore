package com.versiobit.objectstore

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.reduce
import kotlin.math.min

/**
 * A generic abstraction for object-based storage. This interface can be backed by any storage system
 * that supports basic object operations e.g. local filesystems, cloud storage solutions, or other data stores.
 *
 * The storage is conceptually keyed by strings. Keys may include path-like structures (e.g. "folder/subfolder/file"),
 * but the interface itself does not impose a hierarchical file system model.
 */
interface ObjectStore {

    /**
     * Checks whether an object with the specified [key] exists in this store.
     *
     * @param key The key identifying the object to check.
     * @return `true` if the object exists; `false` otherwise.
     */
    suspend fun doesObjectExist(key: String) = headObject(key) != null

    /**
     * Checks whether any objects exist under the given [keyPrefix].
     * This method can be used to determine if there is at least one object
     * whose key starts with the provided prefix.
     *
     * @param keyPrefix The prefix to check (e.g. a folder path, or any partial key).
     * @return `true` if at least one object with the [keyPrefix] exists; `false` otherwise.
     */
    suspend fun doAnyObjectsExist(keyPrefix: String): Boolean

    /**
     * Retrieves the object identified by [key] as a [ByteArray], returning `null` if the object
     * does not exist.
     *
     * @param key The key identifying the object to retrieve.
     * @return The object's contents as a [ByteArray], or `null` if not found.
     */
    suspend fun getObjectAsByteArrayOrNull(key: String): ByteArray?

    /**
     * Retrieves the object identified by [key] as a [ByteArray], throwing an [ObjectNotFoundException]
     * if it does not exist.
     *
     * @param key The key identifying the object to retrieve.
     * @return The object's contents as a [ByteArray].
     * @throws ObjectNotFoundException if the object does not exist.
     */
    suspend fun getObjectAsByteArray(key: String): ByteArray =
        getObjectAsByteArrayOrNull(key) ?: throw ObjectNotFoundException("Object '$key' not found")

    /**
     * Returns the object at [key] as a [Flow] of [ByteArray] chunks or `null` if the object does not exist.
     * This method can be useful for streaming large files without loading them fully into memory at once.
     *
     * @param key The key identifying the object to retrieve.
     * @param chunkSize The size of each data chunk in bytes.
     * @return A [Flow] that emits chunks of the object data, or `null` if not found.
     */
    suspend fun getObjectAsFlowOrNull(key: String, chunkSize: Int): Flow<ByteArray>? {
        val buffer = getObjectAsByteArrayOrNull(key)
        return if (buffer != null) flow {
            var currentPos = 0
            do {
                val endPos = min(currentPos + chunkSize, buffer.size)
                emit(buffer.copyOfRange(currentPos, endPos))
                currentPos = endPos
            } while (endPos != buffer.size)
        } else null
    }

    /**
     * Returns the object at [key] as a [Flow] of [ByteArray] chunks, or throws an [ObjectNotFoundException]
     * if the object does not exist.
     *
     * @param key The key identifying the object to retrieve.
     * @param chunkSize The size of each data chunk in bytes.
     * @return A [Flow] that emits chunks of the object data.
     * @throws ObjectNotFoundException if the object does not exist.
     */
    suspend fun getObjectAsFlow(key: String, chunkSize: Int): Flow<ByteArray> =
        getObjectAsFlowOrNull(key, chunkSize) ?: throw ObjectNotFoundException("Object '$key' not found")

    /**
     * Returns metadata for an object, including information such as size, or `null` if the object does not exist.
     *
     * @param key The key identifying the object to inspect.
     * @return An [ObjectInfo] object containing metadata (e.g. size), or `null` if not found.
     */
    suspend fun headObject(key: String): ObjectInfo? =
        getObjectAsByteArrayOrNull(key)?.let { ObjectInfo(it.size.toLong()) }

    /**
     * Stores the data from [data] under the specified [key]. If an object already exists at this key, it will be
     * overwritten.
     *
     * @param key The key under which to store the data.
     * @param data The bytes to store.
     */
    suspend fun putObject(key: String, data: ByteArray)

    /**
     * Stores data provided as a [Flow] of [ByteArray] chunks.
     * Useful for uploading large files without loading them fully into memory at once.
     *
     * @param key The key under which to store the data.
     * @param data A [Flow] of byte chunks to store.
     */
    suspend fun putObject(key: String, data: Flow<ByteArray>) {
        val buffer = data.reduce { a, v -> a + v }
        putObject(key, buffer)
    }

    /**
     * Deletes the object identified by [key]. If the object does not exist, this operation does nothing.
     *
     * @param key The key identifying the object to delete.
     */
    suspend fun deleteObject(key: String)

    /**
     * Deletes a list of objects identified by [keys].
     * If any of the objects do not exist, those deletions are simply no-ops.
     *
     * @param keys The keys identifying the objects to delete.
     */
    suspend fun deleteObjects(keys: List<String>) {
        keys.forEach { deleteObject(it) }
    }

    /**
     * Recursively deletes all objects underneath the specified [keyPrefix].
     * This typically means deleting any objects whose keys start with [keyPrefix].
     *
     * @param keyPrefix The prefix under which to delete objects.
     */
    suspend fun deleteRecursive(keyPrefix: String)

    /**
     * Copies a single object from the [sourceObjectStore] with [sourceKey] into this store, placing it at [destinationKey].
     *
     * The default implementation loads the source object as bytes and writes them to the destination.
     * Implementing classes may optimize this if the source store is of the same type.
     *
     * @param sourceObjectStore The store from which to read the object.
     * @param sourceKey The key identifying the source object.
     * @param destinationKey The key under which to store the copied object in this store.
     * @throws ObjectNotFoundException if the source object does not exist.
     */
    suspend fun copyObjectFrom(sourceObjectStore: ObjectStore, sourceKey: String, destinationKey: String) {
        val sourceObject = sourceObjectStore.getObjectAsByteArray(sourceKey)
        putObject(destinationKey, sourceObject)
    }

    /**
     * Recursively copies all objects from [srcKeyPrefix] to [destKeyPrefix] within this store.
     * The definition of recursion here generally means “all objects with keys starting with [srcKeyPrefix].”
     *
     * @param srcKeyPrefix The prefix identifying which objects to copy from this store.
     * @param destKeyPrefix The prefix to which the objects should be copied.
     */
    suspend fun copyRecursive(srcKeyPrefix: String, destKeyPrefix: String)
}
