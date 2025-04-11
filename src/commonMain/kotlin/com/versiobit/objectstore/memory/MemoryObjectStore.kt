package com.versiobit.objectstore.memory

import com.versiobit.objectstore.ObjectInfo
import com.versiobit.objectstore.ObjectStore
import com.versiobit.objectstore.concurrentHashMap

class MemoryObjectStore : ObjectStore {

    private val objects = concurrentHashMap<String, ByteArray>()

    override suspend fun doAnyObjectsExist(keyPrefix: String): Boolean {
        return objects.any { it.key.startsWith(keyPrefix) }
    }

    override suspend fun doesObjectExist(key: String): Boolean {
        return objects.containsKey(key)
    }

    override suspend fun getObjectAsByteArrayOrNull(key: String): ByteArray? {
        return objects[key]
    }

    override suspend fun headObject(key: String): ObjectInfo? {
        return objects[key]?.let { ObjectInfo(it.size.toLong()) }
    }

    override suspend fun putObject(key: String, data: ByteArray) {
        objects[key] = data
    }

    override suspend fun deleteObject(key: String) {
        objects.remove(key)
    }

    override suspend fun deleteRecursive(keyPrefix: String) {
        val iter = objects.iterator()
        while (iter.hasNext()) {
            val entry = iter.next()
            if (entry.key.startsWith(keyPrefix)) iter.remove()
        }
    }

    override suspend fun copyRecursive(srcKeyPrefix: String, destKeyPrefix: String) {
        val iter = objects.toMap().iterator()
        while (iter.hasNext()) {
            val entry = iter.next()
            if (entry.key.startsWith(srcKeyPrefix)) {
                val destKey = entry.key.replaceFirst(srcKeyPrefix, destKeyPrefix)
                objects[destKey] = entry.value
            }
        }
    }
}
