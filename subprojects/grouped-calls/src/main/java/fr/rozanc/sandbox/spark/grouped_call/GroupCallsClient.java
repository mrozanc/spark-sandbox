package fr.rozanc.sandbox.spark.grouped_call;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

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
import java.util.stream.Collectors;

public class GroupCallsClient implements Serializable {

    private static final int DEFAULT_BUFFER_ALLOCATION = 250;
    private final ExternalService externalService;
    private final StructType outputSchema;
    private final int nbPartitions;
    private final int maxElementsByCall;

    public GroupCallsClient(final ExternalService externalService,
                            final StructType outputSchema,
                            final int nbPartitions,
                            final int maxElementsByCall) {
        this.externalService = externalService;
        this.outputSchema = outputSchema;
        this.nbPartitions = nbPartitions;
        this.maxElementsByCall = maxElementsByCall;
    }

    public Dataset<Row> enrichWithRemoteData(final Dataset<Row> dataset) {
        final StructType keySchema = new StructType(new StructField[]{
                new StructField("value", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("negative_value", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("call_id", DataTypes.LongType, true, Metadata.empty())
        });

//        final Dataset<Row> keyDataset = dataset.select("value")
//                .distinct()
//                .repartition(nbPartitions)
//                .mapPartitions((MapPartitionsFunction<Row, Row>) (rows -> callExternalService(rows, externalService, keySchema, maxElementsByCall)), RowEncoder.apply(keySchema));
//
//        return dataset.join(keyDataset, JavaConverters.asScalaBuffer(Collections.singletonList("value")).toSeq());

//        final Dataset<Row> keyDataset = dataset.select("id", "value")
//                .repartition(nbPartitions, functions.col("value"))
//                .sortWithinPartitions(functions.col("value"))
//                .mapPartitions((MapPartitionsFunction<Row, Row>) (rows -> callExternalService(rows, externalService, keySchema, maxElementsByCall)), RowEncoder.apply(keySchema));
//
//        return dataset.join(keyDataset, JavaConverters.asScalaBuffer(Arrays.asList("id", "value")).toSeq());
        return dataset
                .repartition(nbPartitions, functions.col("value"))
                .sortWithinPartitions(functions.col("value"))
                .mapPartitions((MapPartitionsFunction<Row, Row>) (rows -> callExternalService(rows, externalService, outputSchema, maxElementsByCall)), RowEncoder.apply(outputSchema));

    }

    public static Iterator<Row> callExternalService(final Iterator<Row> rows,
                                                    final ExternalService externalService,
                                                    final StructType outputSchema,
                                                    final int maxElementsByCall) {
        if (rows == null || !rows.hasNext()) {
            return Collections.emptyIterator();
        }

        final List<Row> outputRows = new ArrayList<>(maxElementsByCall == 0 ? DEFAULT_BUFFER_ALLOCATION : maxElementsByCall);
        final Set<Integer> inputValues = new HashSet<>();
        final List<Row> bufferRows = new ArrayList<>(maxElementsByCall == 0 ? DEFAULT_BUFFER_ALLOCATION : maxElementsByCall);
        Row currentRow;
        Integer previousValue = null;
        while (rows.hasNext()) {
            currentRow = rows.next();
            final int currentValue = currentRow.getInt(currentRow.fieldIndex("value"));
            final boolean isNewValue = previousValue == null || currentValue != previousValue;
            if (!isNewValue || inputValues.size() < maxElementsByCall) {
                inputValues.add(currentValue);
                bufferRows.add(currentRow);
            }
            final boolean currentIsOnNextBuffer = !inputValues.contains(currentValue);
            if (inputValues.size() == maxElementsByCall && isNewValue && currentIsOnNextBuffer || !rows.hasNext()) {
                final ExternalServiceResponse response = externalService.transformValue(new ArrayList<>(inputValues));
                outputRows.addAll(
                        bufferRows.stream()
                                .map(row -> copyAndSet(row, outputSchema, ImmutableMap.of("negative_value", response.getTransformedValues().get(row.getInt(row.fieldIndex("value"))),
                                                                                          "call_id", response.getCallNumber())))
                                .collect(Collectors.toList())
                );
                inputValues.clear();
                bufferRows.clear();
                inputValues.add(currentValue);
                bufferRows.add(currentRow);
                if (!rows.hasNext() && currentIsOnNextBuffer) { // Case of the last row if it was not added to the buffer
                    final ExternalServiceResponse lastResponse = externalService.transformValue(new ArrayList<>(inputValues));
                    outputRows.addAll(
                            bufferRows.stream()
                                    .map(row -> copyAndSet(row, outputSchema, ImmutableMap.of("negative_value", lastResponse.getTransformedValues().get(row.getInt(row.fieldIndex("value"))),
                                                                                              "call_id", lastResponse.getCallNumber())))
                                    .collect(Collectors.toList())
                    );
                }
            }
            previousValue = currentValue;
        }

        return outputRows.iterator();
    }

    private static Row copyAndSet(final Row originalRow, final StructType outputSchema, final Map<String, Object> values) {
        final Map<String, Object> originalData = new HashMap<>(JavaConverters.mapAsJavaMap(
                originalRow.getValuesMap(JavaConverters.asScalaBuffer(Arrays.asList(originalRow.schema().fieldNames())).toSeq())));
        originalData.putAll(values);
        return new GenericRowWithSchema(Arrays.stream(outputSchema.fieldNames()).map(originalData::get).toArray(), outputSchema);
    }
}
