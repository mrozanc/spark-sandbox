package fr.rozanc.sandbox.spark.simple

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.toDS
import java.sql.Date
import java.time.LocalDate
import kotlin.random.Random

class SimpleSparkApplication {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val spark = SparkSession.builder()
                .master("local[6]")
                .appName("Simple Spark Application")
                .orCreate

            val data = (1L..1000000L).map { SimpleData(it, Random.nextDouble()) }
            val dataset = spark.toDS(data)
            val outputEncoder = RowEncoder.apply(dataset.schema().add("value_date", DataTypes.DateType))
            dataset
                .toDF()
                .map(MapFunction {
                    it.copyWith(
                        "value_date" to Date.valueOf(LocalDate.now()),
                        "currency" to "USD"
                    )
                }, outputEncoder)
                .show(100, false)
        }
    }
}

data class SimpleData(
    val id: Long,
    val amount: Double = 0.0,
    val currency: String = "EUR",
    val attributes: Map<String, String> = emptyMap()
)

fun Row.copyWith(vararg newValues: Pair<String, Any?>): Row = copyWith(mapOf(*newValues))

fun Row.copyWith(newValues: Map<String, Any?>): Row {
    val allFieldNames = setOf(*schema().fieldNames()) + newValues.keys
    val values = allFieldNames.map { if (newValues.containsKey(it)) newValues[it] else getAs<Any?>(it) }
        .toTypedArray()
    val newSchema = StructType(allFieldNames.map {
        if (newValues.containsKey(it)) JavaTypeInference.inferDataType(newValues[it]?.javaClass)
            .let { inferredType -> StructField(it, inferredType._1, inferredType._2 as Boolean, Metadata.empty()) }
        else schema().fields().first { fieldName -> fieldName.name() == it }
    }.toTypedArray())
    return GenericRowWithSchema(values, newSchema)
}
