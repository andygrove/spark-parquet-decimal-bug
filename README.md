# spark-parquet-decimal-bug

Demonstrates bug where Parquet silently discards values that do not fit into the type definition.

For example, writing 123000.00 to a DecimalType(7,2) results in the entire value being discarded.
