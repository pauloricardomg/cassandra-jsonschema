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
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * CLI tool to validate a YAML (or JSON) file against a JSON Schema.
 */
public class ValidateSchema
{
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    public static void main(String[] args) throws IOException
    {
        if (args.length != 2)
        {
            System.err.println("Usage: validate-schema <schema.json> <cassandra.yaml>");
            System.exit(1);
        }

        File schemaFile = new File(args[0]);
        File dataFile = new File(args[1]);

        Set<ValidationMessage> errors = validate(schemaFile, dataFile);

        if (errors.isEmpty())
        {
            System.out.println("Validation PASSED: " + dataFile.getName() + " is valid against " + schemaFile.getName());
            System.exit(0);
        }
        else
        {
            System.err.println("Validation FAILED: " + errors.size() + " error(s) found:");
            for (ValidationMessage error : errors)
                System.err.println("  - " + error.getInstanceLocation() + ": " + error.getMessage());
            System.exit(1);
        }
    }

    public static Set<ValidationMessage> validate(File schemaFile, File dataFile) throws IOException
    {
        JsonNode schemaNode = JSON_MAPPER.readTree(schemaFile);
        JsonNode dataNode = YAML_MAPPER.readTree(dataFile);
        return validate(schemaNode, dataNode);
    }

    public static Set<ValidationMessage> validate(JsonNode schemaNode, JsonNode dataNode)
    {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        JsonSchema schema = factory.getSchema(schemaNode);
        return schema.validate(dataNode);
    }
}
