package fr.rozanc.sandbox.spark.grouped_call;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
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

public class GroupCallsClient implements Serializable {

    private final ExternalService externalService;
    private final StructType outputSchema;
    private final int nbPartitions;

    public GroupCallsClient(final ExternalService externalService, final StructType outputSchema, final int nbPartitions) {
        this.externalService = externalService;
        this.outputSchema = outputSchema;
        this.nbPartitions = nbPartitions;
    }

    public Dataset<Row> enrichWithRemoteData(final Dataset<Row> dataset) {
        return dataset
                .repartition(nbPartitions, functions.col("value"))
                .mapPartitions((MapPartitionsFunction<Row, Row>) (rows -> callExternalService(rows, externalService, outputSchema)), RowEncoder.apply(outputSchema));
    }

    public static Iterator<Row> callExternalService(final Iterator<Row> rows, final ExternalService externalService, final StructType outputSchema) {
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
        final ExternalServiceResponse response = externalService.transformValue(new ArrayList<>(inputValues));
        return outputRows.stream()
                .map(row -> copyAndSet(row, outputSchema,
                                       ImmutableMap.of("negative_value", response.getTransformedValues().get(row.getInt(row.fieldIndex("value"))),
                                                       "call_id", response.getCallNumber())))
                .iterator();
    }

    private static Row copyAndSet(final Row originalRow, final StructType outputSchema, final Map<String, Object> values) {
        final Map<String, Object> originalData = new HashMap<>(JavaConverters.mapAsJavaMap(
                originalRow.getValuesMap(JavaConverters.asScalaBuffer(Arrays.asList(originalRow.schema().fieldNames())).toSeq())));
        originalData.putAll(values);
        return new GenericRowWithSchema(Arrays.stream(outputSchema.fieldNames()).map(originalData::get).toArray(), outputSchema);
    }
}
