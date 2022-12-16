# go-kafka-tutorial
Writing producers and consumers for Kafka using Avro as serialization.

## Generate go files from AVRO
Use the tool [gogen-avro](https://github.com/actgardner/gogen-avro). To generate Go source files from AVRO schema files, use:

```
gogen-avro -package=<package of output go files> <output directory> <avro schema files>
```
