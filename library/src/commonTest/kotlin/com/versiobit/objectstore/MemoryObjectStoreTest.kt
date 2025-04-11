package com.versiobit.objectstore

import com.versiobit.objectstore.memory.MemoryObjectStore
import kotlin.test.Test

class MemoryObjectStoreTest : ObjectStoreTests() {

    override fun createObjectStore(): ObjectStore = MemoryObjectStore()

    @Test
    fun setup() {
        // for tests inheritance to work
    }

}
