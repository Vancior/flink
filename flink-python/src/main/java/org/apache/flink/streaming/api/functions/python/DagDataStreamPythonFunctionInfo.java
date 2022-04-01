package org.apache.flink.streaming.api.functions.python;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriConsumer;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/** Hold a list of DAG-structured python function infos. */
public class DagDataStreamPythonFunctionInfo implements Serializable, DataStreamPythonFunctionInfo {

    private static final long serialVersionUID = 1L;

    private final PythonFunction rootPythonFunction;

    private final Map<PythonFunction, Integer> pythonFunctionsWithType;

    private final Map<PythonFunction, Set<Tuple2<PythonFunction, String>>> successorMap;

    private final Map<PythonFunction, String> outputMap;

    public DagDataStreamPythonFunctionInfo(PythonFunction root, int functionType) {
        pythonFunctionsWithType = new HashMap<>();
        successorMap = new HashMap<>();
        outputMap = new HashMap<>();
        rootPythonFunction = root;
        addFunction(rootPythonFunction, functionType);
    }

    public Map<PythonFunction, Integer> getPythonFunctionsWithType() {
        return ImmutableMap.copyOf(pythonFunctionsWithType);
    }

    public void addFunction(PythonFunction function, int functionType) {
        pythonFunctionsWithType.put(function, functionType);
    }

    public void addOutput(PythonFunction function, String output) {
        Preconditions.checkArgument(pythonFunctionsWithType.containsKey(function));
        outputMap.put(function, output);
    }

    public void connectFunction(PythonFunction up, PythonFunction down, String tag) {
        Preconditions.checkArgument(pythonFunctionsWithType.containsKey(up));
        Preconditions.checkArgument(pythonFunctionsWithType.containsKey(down));
        Set<Tuple2<PythonFunction, String>> successors =
                successorMap.computeIfAbsent(up, f -> new HashSet<>());
        successors.add(Tuple2.of(down, tag));
    }

    /**
     * Traverse through the whole python function tree, from root in BFS order.
     *
     * @param consumer a {@link TriConsumer} callback that takes parent function, child function,
     *     edge tag as arguments
     */
    public void traverse(TriConsumer<PythonFunction, PythonFunction, String> consumer) {
        Queue<PythonFunction> queue = new ArrayDeque<>();
        Set<PythonFunction> visited = new HashSet<>();

        queue.add(rootPythonFunction);
        visited.add(rootPythonFunction);
        while (!queue.isEmpty()) {
            PythonFunction function = queue.poll();
            for (Tuple2<PythonFunction, String> successor : successorMap.get(function)) {
                consumer.accept(function, successor.f0, successor.f1);
                if (!visited.contains(successor.f0)) {
                    visited.add(successor.f0);
                    queue.add(successor.f0);
                }
            }
        }
    }
}
