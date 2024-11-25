package be.uclouvain.gepiciad.schemas;

import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

public class Schema {
    public final List<Column> columns;

    public Schema(List<Column> columns) {
        this.columns = columns;
    }

    public List<String> getFieldNames() {
        return columns.stream().map(column -> column.getName()).collect(Collectors.toList());
    }

    public List<DataType> getFieldTypes() {
        return columns.stream().map(column -> column.getDataType()).collect(Collectors.toList());
    }
}
