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
package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Internal
public class GenericRowDataConverter {

    private static final Map<Class<?>, Function<Object, ?>> TYPE_CONVERSIONS = new HashMap<>();

    static {
        TYPE_CONVERSIONS.put(String.class, Object::toString);
        TYPE_CONVERSIONS.put(Integer.class, o -> ((Number) o).intValue());
        TYPE_CONVERSIONS.put(int.class, o -> ((Number) o).intValue());
        TYPE_CONVERSIONS.put(Long.class, o -> ((Number) o).longValue());
        TYPE_CONVERSIONS.put(long.class, o -> ((Number) o).longValue());
        TYPE_CONVERSIONS.put(Double.class, o -> ((Number) o).doubleValue());
        TYPE_CONVERSIONS.put(double.class, o -> ((Number) o).doubleValue());
        TYPE_CONVERSIONS.put(Float.class, o -> ((Number) o).floatValue());
        TYPE_CONVERSIONS.put(float.class, o -> ((Number) o).floatValue());
        TYPE_CONVERSIONS.put(Boolean.class, o -> o);
        TYPE_CONVERSIONS.put(boolean.class, o -> o);
        TYPE_CONVERSIONS.put(Byte.class, o -> ((Number) o).byteValue());
        TYPE_CONVERSIONS.put(byte.class, o -> ((Number) o).byteValue());
        TYPE_CONVERSIONS.put(Short.class, o -> ((Number) o).shortValue());
        TYPE_CONVERSIONS.put(short.class, o -> ((Number) o).shortValue());
        TYPE_CONVERSIONS.put(BigDecimal.class, o -> new BigDecimal(o.toString()));
    }

    public static <T> T convertToObject(GenericRowData row, Class<T> clazz) throws Exception {
        T object = clazz.newInstance();
        Field[] fields = clazz.getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            Object value = getValue(row, i, field.getType());
            field.set(object, value);
        }
        return object;
    }

    private static Object getValue(GenericRowData row, int index, Class<?> type) {
        Object value = row.getField(index);
        if (value == null) {
            return null;
        }
        Function<Object, ?> conversionFunction = TYPE_CONVERSIONS.get(type);
        if (conversionFunction == null) {
            throw new RuntimeException("Unsupported type: " + type.getName());
        }

        return conversionFunction.apply(value);
    }
}
