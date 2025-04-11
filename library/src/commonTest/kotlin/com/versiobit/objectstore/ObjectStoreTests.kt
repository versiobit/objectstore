package com.versiobit.objectstore


import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.*

abstract class ObjectStoreTests {

    @Test
    fun putGetDelete() = runTest {
        val objectStore = createObjectStore()
        val content = "helloworld"

        objectStore.putObject("test1", content.encodeToByteArray())
        assertTrue(objectStore.doesObjectExist("test1"))

        val readContent = objectStore.getObjectAsByteArray("test1").decodeToString()
        assertEquals(content.length, readContent.length)

        objectStore.deleteObject("test1")
        assertFalse(objectStore.doesObjectExist("test1"))
    }

    @Test
    fun copyObject() = runTest {
        val objectStore = createObjectStore()
        val content = "helloworld"

        objectStore.putObject("test/copyObject1", content.encodeToByteArray())
        objectStore.copyObjectFrom(objectStore, "test/copyObject1", "test/copyObject2")

        val obj2 = objectStore.getObjectAsByteArray("test/copyObject2").decodeToString()
        assertEquals(content, obj2)
    }

    @Test
    fun copyRecursive() = runTest {
        val objectStore = createObjectStore()
        val content = "helloworld"

        objectStore.putObject("test/copyObject1", content.encodeToByteArray())
        objectStore.copyRecursive("test/", "test2/")

        val obj2 = objectStore.getObjectAsByteArray("test2/copyObject1").decodeToString()
        assertEquals(content, obj2)
    }

    @Test
    fun getMissing() = runTest {
        val objectStore = createObjectStore()
        assertNull(objectStore.getObjectAsByteArrayOrNull("doesnotexist"))
    }

    @Test
    fun getAsChunks() = runTest {
        val objectStore = createObjectStore()
        val content = "abcdefg"
        objectStore.putObject("test1", content.encodeToByteArray())

        val chunks = objectStore.getObjectAsFlow("test1", 3).toList().map { it.decodeToString() }
        assertEquals(3, chunks.size)
        assertEquals("abc", chunks[0])
        assertEquals("def", chunks[1])
        assertEquals("g", chunks[2])
    }

    @Test
    fun putAsChunks() = runTest {
        val objectStore = createObjectStore()
        objectStore.putObject("test1", flow {
            emit("abc".encodeToByteArray())
            emit("def".encodeToByteArray())
            emit("g".encodeToByteArray())
        })
        assertEquals("abcdefg", objectStore.getObjectAsByteArray("test1").decodeToString())
    }

    abstract fun createObjectStore(): ObjectStore

}
