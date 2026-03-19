/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.jsonschema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * CLI tool that generates a JSON Schema from a Cassandra JAR's Config.java.
 */
public class GenerateSchema
{
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.println("Usage: generate-schema <cassandra.jar> <output.json>");
            System.exit(1);
        }
        File jarFile = new File(args[0]);
        File outputFile = new File(args[1]);

        ObjectNode schema = generate(jarFile);
        MAPPER.writeValue(outputFile, schema);
        System.out.println("Generated schema: " + outputFile);
    }

    public static ObjectNode generate(File jarFile) throws Exception
    {
        ConfigIntrospector introspector = new ConfigIntrospector(jarFile);
        List<FieldInfo> fields = introspector.introspect();
        TypeMapper typeMapper = new TypeMapper();

        ObjectNode root = MAPPER.createObjectNode();
        root.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        root.put("title", "Apache Cassandra Configuration");
        root.put("type", "object");

        ObjectNode properties = root.putObject("properties");

        for (FieldInfo field : fields)
        {
            ObjectNode fieldSchema;
            // Use field-name-aware handling for ParameterizedClass to control array form
            if (field.rawType().getSimpleName().equals("ParameterizedClass"))
                fieldSchema = CassandraTypeHandlers.handleParameterizedClass(field.name());
            else
                fieldSchema = typeMapper.mapType(field.genericType());

            if (field.nullable())
                makeNullable(fieldSchema);

            if (field.deprecated())
                fieldSchema.put("deprecated", true);

            properties.set(field.name(), fieldSchema);

            // Add deprecated old names as aliases
            for (String oldName : field.replacesOldNames())
            {
                ObjectNode aliasSchema = fieldSchema.deepCopy();
                aliasSchema.put("deprecated", true);
                aliasSchema.put("description", "Deprecated. Use '" + field.name() + "' instead.");
                properties.set(oldName, aliasSchema);
            }
        }

        root.put("additionalProperties", false);

        // Add $defs if any nested types were generated
        Map<String, ObjectNode> defs = typeMapper.getDefs();
        if (!defs.isEmpty())
        {
            ObjectNode defsNode = root.putObject("$defs");
            for (Map.Entry<String, ObjectNode> entry : defs.entrySet())
                defsNode.set(entry.getKey(), entry.getValue());
        }

        return root;
    }

    private static void makeNullable(ObjectNode schema)
    {
        // If schema has "type", convert to array including "null"
        if (schema.has("type") && schema.get("type").isTextual())
        {
            String existingType = schema.get("type").asText();
            ArrayNode typeArray = MAPPER.createArrayNode();
            typeArray.add(existingType);
            typeArray.add("null");
            schema.set("type", typeArray);
        }
        else if (schema.has("oneOf"))
        {
            // Add null to oneOf
            ArrayNode oneOf = (ArrayNode) schema.get("oneOf");
            ObjectNode nullType = MAPPER.createObjectNode();
            nullType.put("type", "null");
            oneOf.add(nullType);
        }
        else if (schema.has("$ref"))
        {
            // Wrap in oneOf with $ref and null
            String ref = schema.get("$ref").asText();
            schema.remove("$ref");
            ArrayNode oneOf = schema.putArray("oneOf");
            ObjectNode refNode = MAPPER.createObjectNode();
            refNode.put("$ref", ref);
            oneOf.add(refNode);
            ObjectNode nullNode = MAPPER.createObjectNode();
            nullNode.put("type", "null");
            oneOf.add(nullNode);
        }
    }
}
