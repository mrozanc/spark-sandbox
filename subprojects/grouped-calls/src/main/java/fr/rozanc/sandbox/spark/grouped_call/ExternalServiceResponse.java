package fr.rozanc.sandbox.spark.grouped_call;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public class ExternalServiceResponse implements Serializable {

    private long callNumber;
    private Map<Integer, Integer> transformedValues;
}
