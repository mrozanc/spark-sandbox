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
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.spark.sql.functions.ceil;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

class GroupCallsClientTest {

    @Test
    void testGroupedCalls() {
        val sparkSession = SparkSession.builder()
                .master("local[6]")
                .getOrCreate();

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

        final int maxElementsByCall = 100;
        val nbPartitions = 100;
        val datasetSize = 1_000_000L;
        val dataPoolSize = 20000;
        val rand = new Random();

        val client = new GroupCallsClient(new TestExternalService(), outputSchema, nbPartitions, maxElementsByCall);

        List<Row> inputData = LongStream.range(0, datasetSize).boxed()
                .map((rowId) -> new GenericRowWithSchema(new Object[]{rowId, rand.nextInt(dataPoolSize)}, inputSchema))
                .collect(Collectors.toList());

        val inputDataset = sparkSession.createDataset(inputData, RowEncoder.apply(inputSchema));

        val outputDataset = client.enrichWithRemoteData(inputDataset).persist();
        final long numberOfOutputRows = outputDataset.count();
        final long numberOfUnprocessedRows = outputDataset
                .filter((col("value").notEqual(lit(0).minus(col("negative_value")))))
                .count();

        val softly = new SoftAssertions();

        softly.assertThat(numberOfOutputRows).isEqualTo(datasetSize);
        softly.assertThat(numberOfUnprocessedRows).isEqualTo(0L);

        final long dataWithMoreCallsThanAllowed = outputDataset.groupBy("call_id").agg(countDistinct("id").as("distinct_ids"), countDistinct("value").as("distinct_values"))
                .filter((lit(maxElementsByCall).notEqual(lit(0))).and(col("distinct_values").gt(lit(maxElementsByCall))))
                .count();

        softly.assertThat(dataWithMoreCallsThanAllowed).isEqualTo(0);

        final long numberOfMultipleCallsByValue = outputDataset.groupBy("value").agg(countDistinct("call_id").as("distinct_call_ids"), count("id").as("nb_rows"))
                .filter(col("distinct_call_ids").gt(ceil(col("nb_rows").divide(lit(maxElementsByCall)))))
                .count();

        outputDataset.unpersist();

        softly.assertThat(numberOfMultipleCallsByValue).isEqualTo(0);

        softly.assertAll();
    }

    static class TestExternalService implements ExternalService {

        private static final AtomicLong callCount = new AtomicLong(0L);

        @Override
        public ExternalServiceResponse transformValue(List<Integer> values) {
            final long callNumber = callCount.incrementAndGet();

            if (values == null) {
                return new ExternalServiceResponse(callNumber, Collections.emptyMap());
            }

            return new ExternalServiceResponse(callNumber, values.stream()
                    .map((value) -> new AbstractMap.SimpleEntry<>(value, -value))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));
        }
    }
}
