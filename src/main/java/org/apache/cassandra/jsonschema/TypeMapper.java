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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps Java types (from reflection) to JSON Schema nodes.
 */
public class TypeMapper
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, ObjectNode> defs = new HashMap<>();
    private final Set<String> visiting = new java.util.HashSet<>();

    public ObjectNode mapType(Type type)
    {
        if (type instanceof Class<?> clazz)
            return mapClass(clazz);

        if (type instanceof ParameterizedType paramType)
            return mapParameterizedType(paramType);

        // Fallback
        return MAPPER.createObjectNode();
    }

    public Map<String, ObjectNode> getDefs()
    {
        return defs;
    }

    private ObjectNode mapClass(Class<?> clazz)
    {
        // Java arrays (e.g. String[])
        if (clazz.isArray())
        {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("type", "array");
            node.set("items", mapClass(clazz.getComponentType()));
            return node;
        }

        // Primitives
        if (clazz == int.class || clazz == long.class)
            return typeNode("integer");
        if (clazz == float.class || clazz == double.class)
            return typeNode("number");
        if (clazz == boolean.class)
            return typeNode("boolean");

        // Boxed types — nullable since they can be null
        if (clazz == Integer.class || clazz == Long.class)
            return nullableTypeNode("integer");
        if (clazz == Float.class || clazz == Double.class)
            return nullableTypeNode("number");
        if (clazz == Boolean.class)
            return nullableTypeNode("boolean");

        if (clazz == String.class)
            return nullableTypeNode("string");

        // Enums
        if (clazz.isEnum())
            return mapEnum(clazz);

        // java.lang.Object — accept any type
        if (clazz == Object.class)
            return MAPPER.createObjectNode();

        // Cassandra special types
        ObjectNode specialSchema = CassandraTypeHandlers.handleSpecialType(clazz);
        if (specialSchema != null)
            return specialSchema;

        // Nested object — introspect recursively
        return mapNestedObject(clazz);
    }

    private ObjectNode mapParameterizedType(ParameterizedType paramType)
    {
        Type rawType = paramType.getRawType();
        if (!(rawType instanceof Class<?> rawClass))
            return MAPPER.createObjectNode();

        Type[] typeArgs = paramType.getActualTypeArguments();

        // List<T>
        if (List.class.isAssignableFrom(rawClass))
        {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("type", "array");
            if (typeArgs.length > 0)
                node.set("items", mapType(typeArgs[0]));
            return node;
        }

        // Set<T>
        if (Set.class.isAssignableFrom(rawClass))
        {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("type", "array");
            node.put("uniqueItems", true);
            if (typeArgs.length > 0)
                node.set("items", mapType(typeArgs[0]));
            return node;
        }

        // Map<String, V>
        if (Map.class.isAssignableFrom(rawClass))
        {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("type", "object");
            if (typeArgs.length > 1)
                node.set("additionalProperties", mapType(typeArgs[1]));
            return node;
        }

        // Fallback: treat raw type as class
        return mapClass(rawClass);
    }

    private ObjectNode mapEnum(Class<?> clazz)
    {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "string");
        ArrayNode enumValues = node.putArray("enum");
        try
        {
            // Use getDeclaredFields to avoid triggering class initialization
            // which may require dependencies not on our classpath
            for (Field f : clazz.getDeclaredFields())
            {
                if (f.isEnumConstant())
                    enumValues.add(f.getName());
            }
        }
        catch (Exception e)
        {
            // fallback — just leave as string
            node.remove("enum");
        }
        return node;
    }

    private ObjectNode mapNestedObject(Class<?> clazz)
    {
        String defName = defName(clazz);

        // Already defined or being visited — return $ref
        if (defs.containsKey(defName) || visiting.contains(defName))
        {
            ObjectNode refNode = MAPPER.createObjectNode();
            refNode.put("$ref", "#/$defs/" + defName);
            return refNode;
        }

        // Mark as visiting to prevent infinite recursion
        visiting.add(defName);

        ObjectNode objectSchema = MAPPER.createObjectNode();
        objectSchema.put("type", "object");
        ObjectNode properties = objectSchema.putObject("properties");

        // Collect all fields from the class hierarchy (including protected/private)
        // to handle classes like EncryptionOptions where YAML-visible fields are protected
        Set<String> seen = new java.util.HashSet<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass())
        {
            for (Field field : c.getDeclaredFields())
            {
                if (Modifier.isStatic(field.getModifiers()))
                    continue;
                if (field.isSynthetic())
                    continue;
                if (seen.contains(field.getName()))
                    continue;
                seen.add(field.getName());

                ObjectNode fieldSchema;
                // require_client_auth is String in Java but YAML serializes as boolean
                if ("require_client_auth".equals(field.getName()) && field.getType() == String.class)
                    fieldSchema = stringOrBooleanNode();
                // Use field-name-aware handling for ParameterizedClass to control array form
                else if (field.getType().getSimpleName().equals("ParameterizedClass"))
                    fieldSchema = CassandraTypeHandlers.handleParameterizedClass(field.getName());
                else
                    fieldSchema = mapType(field.getGenericType());
                properties.set(camelToSnake(field.getName()), fieldSchema);
            }
        }

        objectSchema.put("additionalProperties", false);

        visiting.remove(defName);
        defs.put(defName, objectSchema);

        ObjectNode refNode = MAPPER.createObjectNode();
        refNode.put("$ref", "#/$defs/" + defName);
        return refNode;
    }

    private String defName(Class<?> clazz)
    {
        // Use simple name, replacing $ with _ for inner classes
        return clazz.getName()
                     .replace("org.apache.cassandra.config.", "")
                     .replace("org.apache.cassandra.", "")
                     .replace('$', '_')
                     .replace('.', '_');
    }

    private ObjectNode stringOrBooleanNode()
    {
        ObjectNode node = MAPPER.createObjectNode();
        ArrayNode types = MAPPER.createArrayNode();
        types.add("string");
        types.add("boolean");
        node.set("type", types);
        return node;
    }

    static String camelToSnake(String name)
    {
        // Already snake_case or all lowercase — no conversion needed
        if (!name.chars().anyMatch(Character::isUpperCase))
            return name;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++)
        {
            char c = name.charAt(i);
            if (Character.isUpperCase(c))
            {
                if (i > 0)
                    sb.append('_');
                sb.append(Character.toLowerCase(c));
            }
            else
            {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private ObjectNode typeNode(String type)
    {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", type);
        return node;
    }

    private ObjectNode nullableTypeNode(String type)
    {
        ObjectNode node = MAPPER.createObjectNode();
        ArrayNode types = MAPPER.createArrayNode();
        types.add(type);
        types.add("null");
        node.set("type", types);
        return node;
    }
}
