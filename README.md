# colors-db
Example application that uses [Apache Calcite](https://github.com/apache/calcite) to implement federated query planning and demonstrates various strategies for serializing these plans for consumption by other systems using the [Substrait](https://github.com/substrait-io/substrait) serialization format for relation algebra.

## System
colord-db consists of 3 data sources Red, Green and Blue, along with an execution engine, White, that can connect with all of these sources.

# Other Resources
* https://www.querifylabs.com/blog/assembling-a-query-optimizer-with-apache-calcite
