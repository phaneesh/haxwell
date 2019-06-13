package com.hbase.haxwell.api.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Builder
public class HaxwellRow {

  public enum OperationType {
    PUT,
    DELETE,
    UNKNOWN
  }

  private String tableName;

  private String id;

  private long timestamp;

  private OperationType operation;

  private List<HaxwellColumn> columns;

  public static HaxwellRow create(final byte[] tableName, final byte[] rowKey, final List<Cell> cells,
                                  final OperationType operationType, final long timestamp) {
    final List<HaxwellColumn> columns = cells.stream().map( cell ->
        HaxwellColumn.builder()
            .columnFamily(Bytes.toString(CellUtil.cloneFamily(cell)))
            .columnName(Bytes.toString(CellUtil.cloneQualifier(cell)))
            .value(Bytes.toString(CellUtil.cloneValue(cell)))
            .build()
    ).collect(Collectors.toList());
    return HaxwellRow.builder()
        .tableName(Bytes.toString(tableName))
        .id(Bytes.toString(rowKey))
        .operation(operationType)
        .timestamp(timestamp)
        .columns(columns)
        .build();
  }

}
