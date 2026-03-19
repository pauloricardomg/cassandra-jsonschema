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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypeMapperTest
{
    private final TypeMapper mapper = new TypeMapper();

    @Test
    void testPrimitiveInt()
    {
        ObjectNode schema = mapper.mapType(int.class);
        assertThat(schema.get("type").asText()).isEqualTo("integer");
    }

    @Test
    void testPrimitiveLong()
    {
        ObjectNode schema = mapper.mapType(long.class);
        assertThat(schema.get("type").asText()).isEqualTo("integer");
    }

    @Test
    void testPrimitiveDouble()
    {
        ObjectNode schema = mapper.mapType(double.class);
        assertThat(schema.get("type").asText()).isEqualTo("number");
    }

    @Test
    void testPrimitiveBoolean()
    {
        ObjectNode schema = mapper.mapType(boolean.class);
        assertThat(schema.get("type").asText()).isEqualTo("boolean");
    }

    @Test
    void testBoxedIntegerIsNullable()
    {
        ObjectNode schema = mapper.mapType(Integer.class);
        assertThat(schema.get("type").isArray()).isTrue();
        assertThat(schema.get("type").get(0).asText()).isEqualTo("integer");
        assertThat(schema.get("type").get(1).asText()).isEqualTo("null");
    }

    @Test
    void testString()
    {
        ObjectNode schema = mapper.mapType(String.class);
        assertThat(schema.get("type").isArray()).isTrue();
        assertThat(schema.get("type").get(0).asText()).isEqualTo("string");
        assertThat(schema.get("type").get(1).asText()).isEqualTo("null");
    }

    enum TestEnum { A, B, C }

    @Test
    void testEnum()
    {
        ObjectNode schema = mapper.mapType(TestEnum.class);
        assertThat(schema.get("type").asText()).isEqualTo("string");
        assertThat(schema.has("enum")).isTrue();
        assertThat(schema.get("enum").size()).isEqualTo(3);
        assertThat(schema.get("enum").get(0).asText()).isEqualTo("A");
    }

    @Test
    void testListOfStrings() throws Exception
    {
        // Use a field to get the parameterized type
        java.lang.reflect.Type type = getClass().getDeclaredField("stringList").getGenericType();
        ObjectNode schema = mapper.mapType(type);
        assertThat(schema.get("type").asText()).isEqualTo("array");
        assertThat(schema.has("items")).isTrue();
    }

    @Test
    void testSetOfStrings() throws Exception
    {
        java.lang.reflect.Type type = getClass().getDeclaredField("stringSet").getGenericType();
        ObjectNode schema = mapper.mapType(type);
        assertThat(schema.get("type").asText()).isEqualTo("array");
        assertThat(schema.get("uniqueItems").asBoolean()).isTrue();
    }

    @Test
    void testMapOfStringToInteger() throws Exception
    {
        java.lang.reflect.Type type = getClass().getDeclaredField("stringIntMap").getGenericType();
        ObjectNode schema = mapper.mapType(type);
        assertThat(schema.get("type").asText()).isEqualTo("object");
        assertThat(schema.has("additionalProperties")).isTrue();
    }

    // Fields used to get generic types for testing
    @SuppressWarnings("unused")
    private List<String> stringList;
    @SuppressWarnings("unused")
    private Set<String> stringSet;
    @SuppressWarnings("unused")
    private Map<String, Integer> stringIntMap;
}
