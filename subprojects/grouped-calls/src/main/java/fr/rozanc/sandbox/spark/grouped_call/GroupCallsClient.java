package fr.rozanc.sandbox.spark.grouped_call;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupCallsClient implements Serializable {

    private final ExternalService externalService;
    private final StructType outputSchema;
    private final int nbPartitions;
    private final Integer maxElementsByCall;

    public GroupCallsClient(final ExternalService externalService, final StructType outputSchema, final int nbPartitions, @Nullable final Integer maxElementsByCall) {
        this.externalService = externalService;
        this.outputSchema = outputSchema;
        this.nbPartitions = nbPartitions;
        this.maxElementsByCall = maxElementsByCall;
    }

    public Dataset<Row> enrichWithRemoteData(final Dataset<Row> dataset) {
        return dataset
                .repartition(nbPartitions, functions.col("value"))
                .mapPartitions((MapPartitionsFunction<Row, Row>) (rows -> callExternalService(rows, externalService, outputSchema, maxElementsByCall)), RowEncoder.apply(outputSchema));
    }

    public static Iterator<Row> callExternalService(final Iterator<Row> rows,
                                                    final ExternalService externalService,
                                                    final StructType outputSchema,
                                                    final Integer maxElementsByCall) {
        if (rows == null || !rows.hasNext()) {
            return Collections.emptyIterator();
        }

        final Set<Integer> inputValues = new HashSet<>();
        final List<Row> outputRows = new ArrayList<>();
        Row currentRow;
        while (rows.hasNext()) {
            currentRow = rows.next();
            outputRows.add(currentRow);
            inputValues.add(currentRow.getInt(currentRow.fieldIndex("value")));
        }

        final List<List<Integer>> callsList;
        if (maxElementsByCall == null) {
            callsList = Collections.singletonList(new ArrayList<>(inputValues));
        } else {
            callsList = Lists.partition(new ArrayList<>(inputValues), maxElementsByCall);
        }

        final MergedExternalServiceResponse mergedResponses = callsList.stream()
                .map(externalService::transformValue)
                .collect(MergedExternalServiceResponse::new, (mr, r) -> {
                    r.getTransformedValues().keySet().forEach(value -> mr.getValueToCallIdMap().put(value, r.getCallNumber()));
                    mr.getValueToTransformedValueMap().putAll(r.getTransformedValues());
                }, (mr1, mr2) -> {
                    mr1.getValueToCallIdMap().putAll(mr2.getValueToCallIdMap());
                    mr1.getValueToTransformedValueMap().putAll(mr2.getValueToTransformedValueMap());
                });

        return outputRows.stream()
                .map(row -> copyAndSet(row, outputSchema,
                                       ImmutableMap.of("negative_value", mergedResponses.getValueToTransformedValueMap().get(row.getInt(row.fieldIndex("value"))),
                                                       "call_id", mergedResponses.getValueToCallIdMap().get(row.getInt(row.fieldIndex("value"))))))
                .iterator();
    }

    private static Row copyAndSet(final Row originalRow, final StructType outputSchema, final Map<String, Object> values) {
        final Map<String, Object> originalData = new HashMap<>(JavaConverters.mapAsJavaMap(
                originalRow.getValuesMap(JavaConverters.asScalaBuffer(Arrays.asList(originalRow.schema().fieldNames())).toSeq())));
        originalData.putAll(values);
        return new GenericRowWithSchema(Arrays.stream(outputSchema.fieldNames()).map(originalData::get).toArray(), outputSchema);
    }

    @Data
    private static class MergedExternalServiceResponse implements Serializable {
        private Map<Integer, Long> valueToCallIdMap = new HashMap<>();
        private Map<Integer, Integer> valueToTransformedValueMap = new HashMap<>();
    }
}
