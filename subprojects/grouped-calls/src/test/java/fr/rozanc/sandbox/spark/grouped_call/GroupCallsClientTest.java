package fr.rozanc.sandbox.spark.grouped_call;

import lombok.val;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class GroupCallsClientTest {

    @Test
    void testGroupedCalls() {
        val sparkSession = SparkSession.builder().master("local[6]").getOrCreate();

        val inputSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("value", DataTypes.IntegerType, true, Metadata.empty())
        });

        val outputSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("value", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("negative_value", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("call_id", DataTypes.LongType, true, Metadata.empty())
        });

        val nbPartitions = 100;
        val datasetSize = 100_000L;
        val dataPoolSize = 20000;
        val rand = new Random();

        val client = new GroupCallsClient(new TestExternalService(), outputSchema, nbPartitions);

        List<Row> inputData = LongStream.range(1, datasetSize).boxed()
                .map((rowId) -> new GenericRowWithSchema(new Object[]{rowId, rand.nextInt(dataPoolSize)}, inputSchema))
                .collect(Collectors.toList());

        val inputDataset = sparkSession.createDataset(inputData, RowEncoder.apply(inputSchema));

        val outputDataset = client.enrichWithRemoteData(inputDataset).persist();
        val outputData = outputDataset.collectAsList();

        val softly = new SoftAssertions();

        softly.assertThat(outputData).allSatisfy((row) -> {
            assertThat(row.getInt(row.fieldIndex("negative_value")))
                    .as("#" + row.getLong(row.fieldIndex("id")) + " negative_value")
                    .isEqualTo(-row.getInt(row.fieldIndex("value")));
            assertThat(row.<Long>getAs("call_id")).isNotNull();
        });

        val dataGroupedByCallId = outputDataset.groupBy("call_id").agg(functions.countDistinct("id"), functions.countDistinct("value"))
                .orderBy("call_id")
                .collectAsList();

        softly.assertThat(dataGroupedByCallId).hasSizeLessThanOrEqualTo(nbPartitions);

        val dataGroupedByValue = outputDataset.groupBy("value").agg(functions.countDistinct("call_id"))
                .collectAsList();

        outputDataset.unpersist();

        softly.assertThat(dataGroupedByValue).allSatisfy(row -> {
            assertThat(row.getLong(row.fieldIndex("count(DISTINCT call_id)"))).isEqualTo(1L);
        });

        softly.assertAll();
    }

    static class TestExternalService implements ExternalService {

        private static final AtomicLong callCount = new AtomicLong(0L);

        @Override
        public ExternalServiceResponse transformValue(List<Integer> values) {
            final long callNumer = callCount.incrementAndGet();

            if (values == null) {
                return new ExternalServiceResponse(callNumer, Collections.emptyMap());
            }

            return new ExternalServiceResponse(callNumer, values.stream()
                    .map((value) -> new AbstractMap.SimpleEntry<>(value, -value))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));
        }
    }
}
