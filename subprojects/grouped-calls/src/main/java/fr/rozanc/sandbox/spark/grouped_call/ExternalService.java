package fr.rozanc.sandbox.spark.grouped_call;

import java.io.Serializable;
import java.util.List;

public interface ExternalService extends Serializable {

    ExternalServiceResponse transformValue(List<Integer> value);
}
