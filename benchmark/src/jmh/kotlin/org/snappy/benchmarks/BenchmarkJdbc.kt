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
import org.snappy.EmptyResult
import java.sql.Connection
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class BenchmarkJdbc : BenchmarkBase() {
    @Setup
    open fun start() {
        setup()
    }

    @Benchmark
    open fun queryDataClass() {
        step()
        connection.prepareStatement(query).use { preparedStatement ->
            preparedStatement.setInt(1, id)
            preparedStatement.executeQuery().use { resultSet ->
                if (!resultSet.next()) {
                    throw EmptyResult()
                }
                PostDataClass(
                    resultSet.getInt(1),
                    resultSet.getString(2),
                    resultSet.getObject(3, LocalDateTime::class.java),
                    resultSet.getObject(4, LocalDateTime::class.java),
                    resultSet.getInt(5),
                    resultSet.getInt(6),
                    resultSet.getInt(7),
                    resultSet.getInt(8),
                    resultSet.getInt(9),
                    resultSet.getInt(10),
                    resultSet.getInt(11),
                    resultSet.getInt(12),
                    resultSet.getInt(13),
                )
            }
        }
    }

    @Benchmark
    open fun queryPojoClass() {
        step()
        connection.prepareStatement(query).use { preparedStatement ->
            preparedStatement.setInt(1, id)
            preparedStatement.executeQuery().use { resultSet ->
                if (!resultSet.next()) {
                    throw EmptyResult()
                }
                val post = PostPojoClass()
                post.id = resultSet.getInt(1)
                post.text = resultSet.getString(2)
                post.creationDate = resultSet.getObject(3, LocalDateTime::class.java)
                post.lastChangeDate = resultSet.getObject(4, LocalDateTime::class.java)
                post.counter1 = resultSet.getInt(5)
                post.counter2 = resultSet.getInt(6)
                post.counter3 = resultSet.getInt(7)
                post.counter4 = resultSet.getInt(8)
                post.counter5 = resultSet.getInt(9)
                post.counter6 = resultSet.getInt(10)
                post.counter7 = resultSet.getInt(11)
                post.counter8 = resultSet.getInt(12)
                post.counter9 = resultSet.getInt(13)
                post.counter9 = resultSet.getInt(5)
            }
        }
    }
}