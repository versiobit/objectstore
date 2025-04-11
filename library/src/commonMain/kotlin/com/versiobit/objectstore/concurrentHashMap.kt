package com.versiobit.objectstore

expect fun <K : Any, V : Any> concurrentHashMap(): MutableMap<K, V>
