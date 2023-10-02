package org.snappy.benchmarks

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import org.snappy.SnappyMapper
import org.snappy.query.queryFirst
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class BenchmarkSnappy : BenchmarkBase() {
    @Setup
    open fun start() {
        setup()
        SnappyMapper.loadCache()
    }

    @Benchmark
    open fun queryDataClass() {
        step()
        connection.queryFirst<PostDataClass>(query, listOf(id))
    }

    @Benchmark
    open fun queryPojoClass() {
        step()
        connection.queryFirst<PostPojoClass>(query, listOf(id))
    }
}