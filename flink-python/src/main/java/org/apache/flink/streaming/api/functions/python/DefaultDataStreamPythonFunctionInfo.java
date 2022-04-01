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

package org.apache.flink.streaming.api.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;

/** {@link DefaultDataStreamPythonFunctionInfo} holds a PythonFunction and its function type. */
@Internal
public class DefaultDataStreamPythonFunctionInfo extends PythonFunctionInfo
        implements DataStreamPythonFunctionInfo {
    private static final long serialVersionUID = 2L;

    private static final Object[] EMPTY = new Object[0];

    private final int functionType;

    public DefaultDataStreamPythonFunctionInfo(PythonFunction pythonFunction, int functionType) {
        super(pythonFunction, EMPTY);
        this.functionType = functionType;
    }

    public DefaultDataStreamPythonFunctionInfo(
            PythonFunction pythonFunction,
            DefaultDataStreamPythonFunctionInfo input,
            int functionType) {
        super(pythonFunction, new DefaultDataStreamPythonFunctionInfo[] {input});
        this.functionType = functionType;
    }

    public int getFunctionType() {
        return this.functionType;
    }

    public DefaultDataStreamPythonFunctionInfo copy() {
        if (getInputs().length == 0) {
            return new DefaultDataStreamPythonFunctionInfo(getPythonFunction(), this.functionType);
        } else {
            return new DefaultDataStreamPythonFunctionInfo(
                    getPythonFunction(),
                    ((DefaultDataStreamPythonFunctionInfo) getInputs()[0]).copy(),
                    this.functionType);
        }
    }
}
