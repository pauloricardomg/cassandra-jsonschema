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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads a Cassandra JAR via URLClassLoader and introspects Config.java fields.
 */
public class ConfigIntrospector
{
    private final URLClassLoader classLoader;
    private final Class<?> configClass;

    public ConfigIntrospector(File jarFile) throws Exception
    {
        URL jarUrl = jarFile.toURI().toURL();
        this.classLoader = new URLClassLoader(new URL[]{ jarUrl }, getClass().getClassLoader());
        this.configClass = classLoader.loadClass("org.apache.cassandra.config.Config");
    }

    public List<FieldInfo> introspect()
    {
        return introspectClass(configClass);
    }

    public List<FieldInfo> introspectClass(Class<?> clazz)
    {
        List<FieldInfo> fields = new ArrayList<>();
        for (Field field : clazz.getFields())
        {
            if (Modifier.isStatic(field.getModifiers()))
                continue;

            String name = field.getName();
            boolean nullable = isNullable(field);
            boolean deprecated = isDeprecated(field);
            List<String> replacesOldNames = getReplacesOldNames(field);

            fields.add(new FieldInfo(name, field.getGenericType(), field.getType(), nullable, deprecated, replacesOldNames));
        }
        return fields;
    }

    public URLClassLoader getClassLoader()
    {
        return classLoader;
    }

    public Class<?> getConfigClass()
    {
        return configClass;
    }

    private boolean isNullable(Field field)
    {
        for (Annotation ann : field.getAnnotations())
        {
            if (ann.annotationType().getSimpleName().equals("Nullable"))
                return true;
        }
        // Also check if the field has a null default for reference types
        return false;
    }

    private boolean isDeprecated(Field field)
    {
        return field.isAnnotationPresent(Deprecated.class) || hasAnnotationNamed(field, "Deprecated");
    }

    private List<String> getReplacesOldNames(Field field)
    {
        List<String> oldNames = new ArrayList<>();
        for (Annotation ann : field.getAnnotations())
        {
            String annTypeName = ann.annotationType().getSimpleName();
            if ("Replaces".equals(annTypeName))
            {
                try
                {
                    Method oldNameMethod = ann.annotationType().getMethod("oldName");
                    String oldName = (String) oldNameMethod.invoke(ann);
                    if (oldName != null && !oldName.isEmpty())
                        oldNames.add(oldName);
                }
                catch (Exception e)
                {
                    // ignore
                }
            }
            // Handle @ReplacesList which contains multiple @Replaces
            if ("ReplacesList".equals(annTypeName))
            {
                try
                {
                    Method valueMethod = ann.annotationType().getMethod("value");
                    Object[] replaces = (Object[]) valueMethod.invoke(ann);
                    for (Object r : replaces)
                    {
                        Method oldNameMethod = r.getClass().getMethod("oldName");
                        String oldName = (String) oldNameMethod.invoke(r);
                        if (oldName != null && !oldName.isEmpty())
                            oldNames.add(oldName);
                    }
                }
                catch (Exception e)
                {
                    // ignore
                }
            }
        }
        return oldNames;
    }

    private boolean hasAnnotationNamed(Field field, String name)
    {
        for (Annotation ann : field.getAnnotations())
        {
            if (ann.annotationType().getSimpleName().equals(name))
                return true;
        }
        return false;
    }
}
