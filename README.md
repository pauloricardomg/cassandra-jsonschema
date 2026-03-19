# Cassandra JSON Schema Generator

Generates [JSON Schema](https://json-schema.org/) from Apache Cassandra's `Config.java` via reflection, enabling automated validation of `cassandra.yaml` configuration files.

## Features

- **Version-agnostic**: Works with any Cassandra JAR (tested with 3.11, 4.1, 5.0, 5.1)
- **No compile dependency on Cassandra**: Loads the JAR at runtime via `URLClassLoader`
- **Full type support**: Primitives, enums, `DurationSpec`, `DataStorageSpec`, `DataRateSpec`, `ParameterizedClass`, collections, maps, and nested config objects
- **Draft 2020-12 JSON Schema**: Modern schema format with `$defs` and `$ref`

## Prerequisites

- Java 21 (for running the tools)
- Maven 3.8+
- For `generate-schema-from-source`: Git, Ant, and the appropriate JDK for the Cassandra version (Java 8 for 3.x, Java 11 for 4.x+)

## Build

```bash
mvn clean package
```

## Usage

### Generate Schema from Source

Clones a Cassandra version from GitHub, builds the JAR, and generates the schema — no pre-built JAR needed. Automatically selects the correct JDK (Java 8 for 3.x, Java 11 for 4.x and later).

```bash
# From a release tag
bin/generate-schema-from-source 5.0.6 examples/schema-5.0.json

# From a branch
bin/generate-schema-from-source trunk examples/schema-trunk.json
```

### Generate Schema from a JAR

If you already have a Cassandra JAR:

```bash
bin/generate-schema lib/apache-cassandra-5.1-SNAPSHOT.jar examples/schema-5.1.json
```

### Validate Configuration

```bash
bin/validate-schema examples/schema-5.1.json examples/cassandra-5.1-SNAPSHOT.yaml
```

### Multi-Version Examples

```bash
# Cassandra 3.11
bin/generate-schema lib/apache-cassandra-3.11.19-SNAPSHOT.jar examples/schema-3.11.json
bin/validate-schema examples/schema-3.11.json examples/cassandra-3.11.19-SNAPSHOT.yaml

# Cassandra 4.1
bin/generate-schema lib/apache-cassandra-4.1.10-SNAPSHOT.jar examples/schema-4.1.json
bin/validate-schema examples/schema-4.1.json examples/cassandra-4.1.10-SNAPSHOT.yaml

# Cassandra 5.0
bin/generate-schema lib/apache-cassandra-5.0.7-SNAPSHOT.jar examples/schema-5.0.json
bin/validate-schema examples/schema-5.0.json examples/cassandra-5.0.7-SNAPSHOT.yaml

# Cassandra 5.1
bin/generate-schema lib/apache-cassandra-5.1-SNAPSHOT.jar examples/schema-5.1.json
bin/validate-schema examples/schema-5.1.json examples/cassandra-5.1-SNAPSHOT.yaml
```

## License

Apache License, Version 2.0
