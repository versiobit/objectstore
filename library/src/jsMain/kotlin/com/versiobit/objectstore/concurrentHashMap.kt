package com.versiobit.objectstore

actual fun <K : Any, V : Any> concurrentHashMap(): MutableMap<K, V> = mutableMapOf()
