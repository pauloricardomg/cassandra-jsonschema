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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.ValidationMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidateSchemaTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testValidDataPasses() throws Exception
    {
        JsonNode schema = MAPPER.readTree("""
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "count": {"type": "integer"}
                },
                "additionalProperties": false
            }
            """);
        JsonNode data = MAPPER.readTree("""
            {"name": "test", "count": 42}
            """);

        Set<ValidationMessage> errors = ValidateSchema.validate(schema, data);
        assertThat(errors).isEmpty();
    }

    @Test
    void testInvalidDataFails() throws Exception
    {
        JsonNode schema = MAPPER.readTree("""
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "name": {"type": "string"}
                },
                "additionalProperties": false
            }
            """);
        JsonNode data = MAPPER.readTree("""
            {"name": 123}
            """);

        Set<ValidationMessage> errors = ValidateSchema.validate(schema, data);
        assertThat(errors).isNotEmpty();
    }

    @Test
    void testAdditionalPropertiesFails() throws Exception
    {
        JsonNode schema = MAPPER.readTree("""
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "name": {"type": "string"}
                },
                "additionalProperties": false
            }
            """);
        JsonNode data = MAPPER.readTree("""
            {"name": "test", "extra": "field"}
            """);

        Set<ValidationMessage> errors = ValidateSchema.validate(schema, data);
        assertThat(errors).isNotEmpty();
    }
}
