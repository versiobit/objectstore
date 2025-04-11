package com.versiobit.objectstore

import java.util.concurrent.ConcurrentHashMap

actual fun <K : Any, V : Any> concurrentHashMap(): MutableMap<K, V> = ConcurrentHashMap()
