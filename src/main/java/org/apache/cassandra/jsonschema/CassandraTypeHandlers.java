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

import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Handles Cassandra-specific types: DurationSpec, DataStorageSpec, DataRateSpec, ParameterizedClass.
 */
public class CassandraTypeHandlers
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Fields where Cassandra's YAML loader accepts an array of ParameterizedClass
    // even though the Java type is a single ParameterizedClass
    private static final Set<String> ARRAY_PARAMETERIZED_CLASS_FIELDS = Set.of(
        "seed_provider", "crypto_provider", "back_pressure_strategy",
        "key_provider", "logger",
        "commitlog_compression", "hints_compression"
    );

    /**
     * Checks if the class name matches a known Cassandra spec type and returns
     * the appropriate schema, or null if not handled.
     */
    public static ObjectNode handleSpecialType(Class<?> clazz)
    {
        String className = clazz.getName();

        if (className.contains("DurationSpec"))
            return createSpecSchema("Duration value. Accepts a string (e.g., '10s', '500ms', '1h') or an integer.");

        if (className.contains("DataStorageSpec"))
            return createSpecSchema("Data storage value. Accepts a string (e.g., '256MiB', '1024KiB') or an integer.");

        if (className.contains("DataRateSpec"))
            return createSpecSchema("Data rate value. Accepts a string (e.g., '64MiB/s', '1000B/s') or an integer.");

        if (clazz.getSimpleName().equals("ParameterizedClass"))
            return createParameterizedClassSchema(false);

        // MaxAttempt: wrapper around int, YAML accepts a plain integer
        if (clazz.getSimpleName().equals("MaxAttempt"))
            return nullableIntegerNode();

        // SubnetGroups: Cassandra deserializes a list of plain strings via constructor
        if (clazz.getSimpleName().equals("SubnetGroups"))
            return createSubnetGroupsSchema();

        // SubnetGroups.Group: serialized as a plain string in YAML
        if (clazz.getSimpleName().equals("Group") && className.contains("SubnetGroups"))
            return typeNode("string");

        // OptionaldPositiveInt: wrapper around int, YAML accepts a plain integer
        if (clazz.getSimpleName().equals("OptionaldPositiveInt"))
            return nullableIntegerNode();

        // CustomGuardrailConfig extends HashMap<String, Object> — accept any keys
        if (clazz.getSimpleName().equals("CustomGuardrailConfig"))
        {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("type", "object");
            node.put("additionalProperties", true);
            return node;
        }

        return null;
    }

    /**
     * Returns the ParameterizedClass schema for a specific field name,
     * allowing the array form only for fields known to use it in YAML.
     */
    public static ObjectNode handleParameterizedClass(String fieldName)
    {
        return createParameterizedClassSchema(ARRAY_PARAMETERIZED_CLASS_FIELDS.contains(fieldName));
    }

    /**
     * Returns true if the type is a Cassandra spec type (Duration, DataStorage, DataRate, ParameterizedClass).
     */
    public static boolean isSpecialType(Class<?> clazz)
    {
        String className = clazz.getName();
        return className.contains("DurationSpec")
               || className.contains("DataStorageSpec")
               || className.contains("DataRateSpec")
               || clazz.getSimpleName().equals("ParameterizedClass")
               || clazz.getSimpleName().equals("MaxAttempt")
               || clazz.getSimpleName().equals("OptionaldPositiveInt")
               || clazz.getSimpleName().equals("CustomGuardrailConfig")
               || className.contains("SubnetGroups");
    }

    private static ObjectNode nullableIntegerNode()
    {
        ObjectNode node = MAPPER.createObjectNode();
        ArrayNode types = MAPPER.createArrayNode();
        types.add("integer");
        types.add("null");
        node.set("type", types);
        return node;
    }

    private static ObjectNode createSubnetGroupsSchema()
    {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "object");
        ObjectNode properties = node.putObject("properties");
        ObjectNode subnets = MAPPER.createObjectNode();
        subnets.put("type", "array");
        subnets.set("items", typeNode("string"));
        properties.set("subnets", subnets);
        node.put("additionalProperties", false);
        return node;
    }

    private static ObjectNode typeNode(String type)
    {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", type);
        return node;
    }

    private static ObjectNode createSpecSchema(String description)
    {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("description", description);
        ArrayNode oneOf = node.putArray("oneOf");

        ObjectNode stringType = MAPPER.createObjectNode();
        stringType.put("type", "string");
        oneOf.add(stringType);

        ObjectNode intType = MAPPER.createObjectNode();
        intType.put("type", "integer");
        oneOf.add(intType);

        ObjectNode nullType = MAPPER.createObjectNode();
        nullType.put("type", "null");
        oneOf.add(nullType);

        return node;
    }

    private static ObjectNode createParameterizedClassSchema(boolean allowArray)
    {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("description", "Pluggable class with optional parameters.");

        ArrayNode oneOf = node.putArray("oneOf");

        // A plain string (just the class name, e.g., "AllowAllAuthenticator")
        ObjectNode stringForm = MAPPER.createObjectNode();
        stringForm.put("type", "string");
        oneOf.add(stringForm);

        // An object {class_name: ..., parameters: ...}
        oneOf.add(createParameterizedClassObjectSchema());

        // An array of objects — only for fields that use this form in YAML
        if (allowArray)
        {
            ObjectNode arrayForm = MAPPER.createObjectNode();
            arrayForm.put("type", "array");
            arrayForm.set("items", createParameterizedClassObjectSchema());
            oneOf.add(arrayForm);
        }

        return node;
    }

    private static ObjectNode createParameterizedClassObjectSchema()
    {
        ObjectNode obj = MAPPER.createObjectNode();
        obj.put("type", "object");

        ObjectNode properties = obj.putObject("properties");

        ObjectNode className = MAPPER.createObjectNode();
        className.put("type", "string");
        className.put("description", "Fully qualified class name");
        properties.set("class_name", className);

        // Parameters can be: a map {k:v}, an array of maps [{k:v}], or null
        ObjectNode parameters = MAPPER.createObjectNode();
        parameters.put("description", "Key-value parameters for the class");
        ArrayNode paramOneOf = parameters.putArray("oneOf");

        // Map with any value type (values can be strings, numbers, booleans)
        ObjectNode mapType = MAPPER.createObjectNode();
        mapType.put("type", "object");
        mapType.put("additionalProperties", true);
        paramOneOf.add(mapType);

        // Array of maps (YAML serialization format); items can be objects or null
        // (a bare YAML "- " produces a null entry)
        ObjectNode arrayType = MAPPER.createObjectNode();
        arrayType.put("type", "array");
        ObjectNode arrayItems = MAPPER.createObjectNode();
        ArrayNode itemTypes = MAPPER.createArrayNode();
        itemTypes.add("object");
        itemTypes.add("null");
        arrayItems.set("type", itemTypes);
        arrayItems.put("additionalProperties", true);
        arrayType.set("items", arrayItems);
        paramOneOf.add(arrayType);

        // null
        ObjectNode nullType = MAPPER.createObjectNode();
        nullType.put("type", "null");
        paramOneOf.add(nullType);

        properties.set("parameters", parameters);

        ArrayNode required = obj.putArray("required");
        required.add("class_name");

        obj.put("additionalProperties", false);

        return obj;
    }
}
