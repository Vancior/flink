/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.util;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** . */
public class PythonConnectorUtils {

    @SuppressWarnings("unchecked")
    public static <T> T createFirstColumnTopicSelector(Class<T> clazz) {
        return (T)
                Proxy.newProxyInstance(
                        clazz.getClassLoader(),
                        new Class[] {clazz},
                        new FirstColumnTopicSelectorInvocationHandler());
    }

    /** . */
    public static class FirstColumnTopicSelectorInvocationHandler
            implements InvocationHandler, Serializable {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Preconditions.checkArgument(method.getName().equals("apply"));
            Preconditions.checkArgument(args.length == 1);
            Row row = (Row) args[0];
            Preconditions.checkArgument(row.getArity() == 2);
            Preconditions.checkArgument(row.getField(0) instanceof String);
            return row.getField(0);
        }
    }

    /** . */
    public static class SecondColumnSerializationSchema<T> implements SerializationSchema<Row> {

        private final SerializationSchema<T> wrappedSchema;

        public SecondColumnSerializationSchema(SerializationSchema<T> wrappedSchema) {
            this.wrappedSchema = wrappedSchema;
        }

        @Override
        public void open(InitializationContext context) throws Exception {
            wrappedSchema.open(context);
        }

        @SuppressWarnings("unchecked")
        @Override
        public byte[] serialize(Row row) {
            Preconditions.checkArgument(row.getArity() == 2);
            return wrappedSchema.serialize((T) row.getField(1));
        }
    }
}
