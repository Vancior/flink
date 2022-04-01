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

package org.apache.flink.python.chain;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.util.PythonConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.python.DagDataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.functions.python.DefaultDataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractDataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.MultiOutputPythonProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonProcessOperator;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.python.chain.PythonOperatorChainingOptimizer.areOperatorsChainableByChainingStrategy;
import static org.apache.flink.python.chain.PythonOperatorChainingOptimizer.getOperatorFactory;
import static org.apache.flink.python.chain.PythonOperatorChainingOptimizer.replaceInput;
import static org.apache.flink.shaded.guava30.com.google.common.collect.Iterables.getOnlyElement;

/**
 * An util class which attempts to chain all available Python functions.
 *
 * <p>An operator could be chained to it's predecessor if all of the following conditions are met:
 *
 * <ul>
 *   <li>Both of them are Python operators
 *   <li>The parallelism, the maximum parallelism and the slot sharing group are all the same
 *   <li>The chaining strategy is ChainingStrategy.ALWAYS and the chaining strategy of the
 *       predecessor isn't ChainingStrategy.NEVER
 *   <li>This partitioner between them is ForwardPartitioner
 * </ul>
 *
 * <p>The properties of the generated chained operator are as following:
 *
 * <ul>
 *   <li>The name is the concatenation of all the names of the chained operators
 *   <li>The parallelism, the maximum parallelism and the slot sharing group are from one of the
 *       chained operators as all of them are the same between the chained operators
 *   <li>The chaining strategy is the same as the head operator
 *   <li>The uid and the uidHash are the same as the head operator
 * </ul>
 */
public class PythonOperatorTransitiveChainingOptimizer {

    private static Integer sideOutputCounter = 0;

    private static Integer getNewSideOutputId() {
        return ++sideOutputCounter;
    }

    /**
     * Perform chaining optimization. It will iterate the transformations defined in the given
     * StreamExecutionEnvironment and update them with the chained transformations.
     */
    @SuppressWarnings("unchecked")
    public static void apply(StreamExecutionEnvironment env) throws Exception {
        if (env.getConfiguration().get(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED)) {
            StreamGraph streamGraph = env.getStreamGraph(false);
            final Field transformationsField =
                    StreamExecutionEnvironment.class.getDeclaredField("transformations");
            transformationsField.setAccessible(true);
            final List<Transformation<?>> transformations =
                    (List<Transformation<?>>) transformationsField.get(env);
            //            transformationsField.set(env, optimize(transformations));
        }
    }

    /**
     * Perform chaining optimization. It will iterate the transformations defined in the given
     * StreamExecutionEnvironment and update them with the chained transformations. Besides, it will
     * return the transformation after chaining optimization for the given transformation.
     */
    @SuppressWarnings("unchecked")
    public static Transformation<?> apply(
            StreamExecutionEnvironment env, Transformation<?> transformation) throws Exception {
        if (env.getConfiguration().get(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED)) {
            final Field transformationsField =
                    StreamExecutionEnvironment.class.getDeclaredField("transformations");
            transformationsField.setAccessible(true);
            final List<Transformation<?>> transformations =
                    (List<Transformation<?>>) transformationsField.get(env);
            //            final Tuple2<List<Transformation<?>>, Transformation<?>> resultTuple =
            //                    optimize(transformations, transformation);
            //            transformationsField.set(env, resultTuple.f0);
            //            return resultTuple.f1;
            return null;
        } else {
            return transformation;
        }
    }

    @SuppressWarnings("unchecked")
    public static List<Transformation<?>> optimize(List<Transformation<?>> transformations) {
        TransformationGraph transformationGraph = new TransformationGraph(transformations);

        Set<TransitiveClosure> initialClosures = new HashSet<>();

        for (TransformationWrapper sourceTransformation : transformationGraph.getSources()) {
            TransitiveClosure closure =
                    new TransitiveClosure(sourceTransformation, transformationGraph);
            sourceTransformation.setClosure(closure);
            initialClosures.add(closure);
        }
        Collection<TransitiveClosure> closures = growClosures(initialClosures, transformationGraph);

        List<Transformation<?>> optimizedTransformations = new ArrayList<>();

        for (TransitiveClosure closure : closures) {
            if (closure.getSuccessors().size() < 1) {
                optimizedTransformations.add(closure.buildBundleTransformation());
                continue;
            }

            // Create intermedia SideOutputTransformation between boundary pairs of a closure
            for (Tuple2<TransformationWrapper, TransformationWrapper> pair :
                    closure.getBoundaryPairs()) {
                TransformationWrapper upTransform = pair.f0;
                TransformationWrapper downTransform = pair.f1;
                Preconditions.checkArgument(
                        !(upTransform.getTransformation() instanceof SideOutputTransformation
                                && downTransform.getTransformation()
                                        instanceof SideOutputTransformation),
                        "Invalid scenario with adjacent SideOutputTransformation");
                if (upTransform.getTransformation() instanceof SideOutputTransformation
                        || downTransform.getTransformation() instanceof SideOutputTransformation
                        || !PythonConfigUtil.isPythonDataStreamOperator(
                                upTransform.getTransformation())) {
                    continue;
                }
                // TODO: this will create side output for non-forking closure too (successors.size()
                // == 1)
                OutputTag<?> outputTag =
                        new OutputTag<>(
                                getNewSideOutputId().toString(),
                                upTransform.getTransformation().getOutputType());
                SideOutputTransformation<?> sideOutputTransformation =
                        new SideOutputTransformation<>(upTransform.getTransformation(), outputTag);

                transformationGraph.insertIntermediateTransformation(
                        sideOutputTransformation, upTransform, downTransform);
                replaceInput(
                        downTransform.getTransformation(),
                        upTransform.getTransformation(),
                        sideOutputTransformation);

                closure.setOutputTag(upTransform, outputTag);

                // TODO: this is for debug, not necessary
                optimizedTransformations.add(sideOutputTransformation);
            }

            Transformation<?> bundleTransformation = closure.buildBundleTransformation();
            optimizedTransformations.add(bundleTransformation);

            // Replace successors' input with bundled transformation
            for (Tuple2<TransformationWrapper, TransformationWrapper> pair :
                    closure.getBoundaryPairs()) {
                TransformationWrapper upTransform = pair.f0;
                TransformationWrapper downTransform = pair.f1;
                replaceInput(
                        downTransform.getTransformation(),
                        upTransform.getTransformation(),
                        bundleTransformation);
            }
        }

        // The root transformation of a closure might have an unbundled transformation as input,
        // when the bundling of the closure containing the input transformation happens after
        // current closure (we don't topologically sort closure). Thus, a calibration of coping
        // updated inputs to bundle transformation is necessary.
        for (TransitiveClosure closure : closures) {
            closure.calibrateBundleTransformation();
        }

        return optimizedTransformations;
    }

    @SuppressWarnings("unchecked")
    public static Tuple2<List<Transformation<?>>, Transformation<?>> optimize(
            List<Transformation<?>> transformations, Transformation<?> targetTransformation)
            throws Exception {
        return null;
    }

    private static Collection<TransitiveClosure> growClosures(
            Collection<TransitiveClosure> initialClosures,
            TransformationGraph transformationGraph) {
        Set<TransitiveClosure> currentRound = new HashSet<>(initialClosures);
        Set<TransitiveClosure> nextRound = new HashSet<>();
        Set<TransitiveClosure> closures = new HashSet<>(initialClosures);

        while (!currentRound.isEmpty()) {
            for (TransitiveClosure closure : currentRound) {
                Set<TransformationWrapper> closureSuccessors = closure.getSuccessors();
                for (TransformationWrapper successor : closureSuccessors) {
                    Set<TransformationWrapper> predecessors =
                            transformationGraph.getPredecessors(successor);
                    if (successor.getClosure() != null || !ifTransformationsVisited(predecessors)) {
                        continue;
                    }
                    if (isChainableWithPredecessors(
                            successor, transformationGraph.getPredecessors(successor))) {
                        closure.addTransformation(successor);
                        successor.setClosure(closure);
                        nextRound.add(closure);
                    } else {
                        TransitiveClosure newClosure =
                                new TransitiveClosure(successor, transformationGraph);
                        successor.setClosure(newClosure);
                        closures.add(newClosure);
                        nextRound.add(newClosure);
                    }
                }
            }
            currentRound.clear();
            currentRound.addAll(nextRound);
            nextRound.clear();
        }

        return closures;
    }

    private static boolean ifTransformationsVisited(
            Collection<TransformationWrapper> transformations) {
        boolean visited = true;
        for (TransformationWrapper transformation : transformations) {
            visited = visited && (transformation.getClosure() != null);
        }
        return visited;
    }

    private static boolean isChainableWithPredecessors(
            TransformationWrapper transformation, Collection<TransformationWrapper> predecessors) {
        boolean chainable = true;
        for (TransformationWrapper predecessor : predecessors) {
            chainable = chainable && isChainable(predecessor, transformation);
        }
        Set<TransitiveClosure> predecessorClosures =
                predecessors.stream()
                        .map(TransformationWrapper::getClosure)
                        .collect(Collectors.toSet());
        return chainable
                && predecessorClosures.size() == 1
                && getOnlyElement(predecessorClosures) != null;
    }

    private static boolean isChainable(TransformationWrapper up, TransformationWrapper down) {
        Transformation<?> upTransform = up.getTransformation();
        Transformation<?> downTransform = down.getTransformation();

        if (upTransform.getParallelism() != downTransform.getParallelism()
                || upTransform.getMaxParallelism() != downTransform.getMaxParallelism()
                || !upTransform.getSlotSharingGroup().equals(downTransform.getSlotSharingGroup())) {
            return false;
        }

        // NOTE: avoid merge python operator into side-output-rooted closure
        if ((isPassthroughTransformation(upTransform)
                        && PythonConfigUtil.isPythonDataStreamOperator(downTransform)
                        && up.getClosure() != null
                        && up.getClosure().getRoot() != up)
                || (isPassthroughTransformation(downTransform)
                        && PythonConfigUtil.isPythonDataStreamOperator(upTransform))) {
            return true;
        }

        // NOTE: only chain python operators
        if (!PythonConfigUtil.isPythonDataStreamOperator(upTransform)
                || !PythonConfigUtil.isPythonDataStreamOperator(downTransform)) {
            return false;
        }

        if (!areOperatorsChainableByChainingStrategy(upTransform, downTransform)) {
            return false;
        }

        final AbstractDataStreamPythonFunctionOperator<?> downOperator =
                (AbstractDataStreamPythonFunctionOperator<?>)
                        ((SimpleOperatorFactory<?>) getOperatorFactory(downTransform))
                                .getOperator();
        // NOTE: this won't happen 'cause there will be a partition transformation before keyed
        // operator
        return downOperator instanceof PythonProcessOperator
                || downOperator instanceof PythonCoProcessOperator;
    }

    private static boolean isPassthroughTransformation(Transformation<?> transformation) {
        return transformation instanceof SideOutputTransformation
                || (transformation instanceof PartitionTransformation
                        && ((PartitionTransformation<?>) transformation).getPartitioner()
                                instanceof ForwardPartitioner);
    }

    private static class TransitiveClosure {

        private final TransformationWrapper rootTransformation;
        private final Set<TransformationWrapper> transformations;
        private final TransformationGraph transformationGraph;
        private final Map<TransformationWrapper, OutputTag<?>> transformationOutputTagMap;
        private Transformation<?> bundleTransformation = null;

        private TransitiveClosure(TransformationWrapper transformation, TransformationGraph graph) {
            rootTransformation = Preconditions.checkNotNull(transformation);
            transformationGraph = Preconditions.checkNotNull(graph);
            transformations = new HashSet<>();
            transformationOutputTagMap = new HashMap<>();
            addTransformation(transformation);
        }

        private void addTransformation(TransformationWrapper transformation) {
            transformations.add(transformation);
        }

        private Set<TransformationWrapper> getTransformations() {
            return ImmutableSet.copyOf(transformations);
        }

        private TransformationWrapper getRoot() {
            return rootTransformation;
        }

        private List<Tuple2<TransformationWrapper, TransformationWrapper>> getBoundaryPairs() {
            List<Tuple2<TransformationWrapper, TransformationWrapper>> pairs = new ArrayList<>();
            for (TransformationWrapper transformation : transformations) {
                for (TransformationWrapper successor :
                        transformationGraph.getSuccessors(transformation)) {
                    if (!transformations.contains(successor)) {
                        pairs.add(Tuple2.of(transformation, successor));
                    }
                }
            }
            return pairs;
        }

        private Set<TransformationWrapper> getSuccessors() {
            Set<TransformationWrapper> result = new HashSet<>();
            for (TransformationWrapper transformation : transformations) {
                result.addAll(transformationGraph.getSuccessors(transformation));
            }
            result.removeAll(transformations);
            return result;
        }

        private Transformation<?> getBundleTransformation() {
            Preconditions.checkNotNull(bundleTransformation);
            return bundleTransformation;
        }

        private void setOutputTag(TransformationWrapper transformation, OutputTag<?> outputTag) {
            transformationOutputTagMap.put(transformation, outputTag);
        }

        /**
         * Build a transformation that bundle all transformations inside the transitive closure into
         * a single transformation. The operator underneath will be the same class as the root
         * transformation, e.g. {@link PythonProcessOperator}. All {@link
         * DefaultDataStreamPythonFunctionInfo} from original operators are condensed into a single
         * {@link org.apache.flink.streaming.api.functions.python.DagDataStreamPythonFunctionInfo}.
         *
         * @return
         */
        @SuppressWarnings("unchecked")
        private Transformation<?> buildBundleTransformation() {
            Preconditions.checkArgument(transformations.size() > 0);
            if (transformations.size() == 1) {
                bundleTransformation = rootTransformation.getTransformation();
                return bundleTransformation;
            }

            Transformation<?> root = rootTransformation.getTransformation();
            Preconditions.checkArgument(
                    root instanceof OneInputTransformation
                            || root instanceof TwoInputTransformation,
                    "Expected root transformation type: OneInputTransformation, "
                            + "TwoInputTransformation. Got "
                            + root.getClass().getSimpleName());
            final AbstractDataStreamPythonFunctionOperator<?> rootOperator =
                    (AbstractDataStreamPythonFunctionOperator<?>)
                            ((SimpleOperatorFactory<?>) getOperatorFactory(root)).getOperator();
            DagDataStreamPythonFunctionInfo dagFunctionInfo =
                    new DagDataStreamPythonFunctionInfo(
                            rootOperator.getPythonFunctionInfo().getPythonFunction(),
                            rootOperator.getPythonFunctionInfo().getFunctionType());

            for (TransformationWrapper wrapper : transformations) {
                Transformation<?> transformation = wrapper.getTransformation();
                if (!(transformation instanceof OneInputTransformation)
                        && !(transformation instanceof TwoInputTransformation)) {
                    continue;
                }
                final AbstractDataStreamPythonFunctionOperator<?> operator =
                        (AbstractDataStreamPythonFunctionOperator<?>)
                                ((SimpleOperatorFactory<?>) getOperatorFactory(transformation))
                                        .getOperator();
                dagFunctionInfo.addFunction(
                        operator.getPythonFunctionInfo().getPythonFunction(),
                        operator.getPythonFunctionInfo().getFunctionType());
            }

            for (TransformationWrapper wrapper : transformations) {
                if (wrapper == rootTransformation) {
                    continue;
                }
                Transformation<?> downTransform = wrapper.getTransformation();
                if (!(downTransform instanceof OneInputTransformation)
                        && !(downTransform instanceof TwoInputTransformation)) {
                    continue;
                }
                final AbstractDataStreamPythonFunctionOperator<?> downOperator =
                        (AbstractDataStreamPythonFunctionOperator<?>)
                                ((SimpleOperatorFactory<?>) getOperatorFactory(downTransform))
                                        .getOperator();
                for (TransformationWrapper predecessor :
                        transformationGraph.getPredecessors(wrapper)) {
                    TransformationWrapper physicalPredecessor = predecessor;
                    String tag = "";
                    while (isPassthroughTransformation(physicalPredecessor.getTransformation())) {
                        if (physicalPredecessor.getTransformation()
                                instanceof SideOutputTransformation) {
                            tag =
                                    ((SideOutputTransformation<?>)
                                                    physicalPredecessor.getTransformation())
                                            .getOutputTag()
                                            .getId();
                        }
                        Preconditions.checkArgument(
                                transformationGraph.getPredecessors(physicalPredecessor).size()
                                        == 1);
                        physicalPredecessor =
                                getOnlyElement(
                                        transformationGraph.getPredecessors(physicalPredecessor));
                    }
                    Transformation<?> upTransform = physicalPredecessor.getTransformation();
                    Preconditions.checkArgument(
                            upTransform instanceof OneInputTransformation
                                    || upTransform instanceof TwoInputTransformation);
                    final AbstractDataStreamPythonFunctionOperator<?> upOperator =
                            (AbstractDataStreamPythonFunctionOperator<?>)
                                    ((SimpleOperatorFactory<?>) getOperatorFactory(upTransform))
                                            .getOperator();
                    dagFunctionInfo.connectFunction(
                            upOperator.getPythonFunctionInfo().getPythonFunction(),
                            downOperator.getPythonFunctionInfo().getPythonFunction(),
                            tag);
                }
            }

            // Add output tag to {@link DagDataStreamPythonFunctionInfo}
            for (Map.Entry<TransformationWrapper, OutputTag<?>> entry :
                    transformationOutputTagMap.entrySet()) {
                TransformationWrapper physicalWrapper = entry.getKey();
                while (isPassthroughTransformation(physicalWrapper.getTransformation())) {
                    Preconditions.checkArgument(
                            transformationGraph.getPredecessors(physicalWrapper).size() == 1);
                    physicalWrapper =
                            getOnlyElement(transformationGraph.getPredecessors(physicalWrapper));
                }
                Transformation<?> physicalTransformation = physicalWrapper.getTransformation();
                Preconditions.checkArgument(
                        physicalTransformation instanceof OneInputTransformation
                                || physicalTransformation instanceof TwoInputTransformation,
                        "Currently only python operator with one or two inputs are supported");
                final AbstractDataStreamPythonFunctionOperator<?> operator =
                        (AbstractDataStreamPythonFunctionOperator<?>)
                                ((SimpleOperatorFactory<?>)
                                                getOperatorFactory(physicalTransformation))
                                        .getOperator();
                dagFunctionInfo.addOutput(
                        operator.getPythonFunctionInfo().getPythonFunction(),
                        entry.getValue().getId());
            }

            MultiOutputPythonProcessOperator bundleOperator =
                    new MultiOutputPythonProcessOperator(
                            rootOperator.getConfiguration(), dagFunctionInfo);

            for (Map.Entry<TransformationWrapper, OutputTag<?>> entry :
                    transformationOutputTagMap.entrySet()) {
                bundleOperator.addOutputTag(entry.getValue());
            }

            if (root instanceof OneInputTransformation) {
                OneInputTransformation<?, ?> oneInputTransformation =
                        (OneInputTransformation<?, ?>) root;
                Transformation<?> upTransform = getOnlyElement(oneInputTransformation.getInputs());
                MultiOutputPythonProcessOperator.OneInputOperator<?> oneInputOperator =
                        bundleOperator.getOneInputOperator(upTransform.getOutputType());
                bundleTransformation =
                        new OneInputTransformation(
                                upTransform,
                                generateName(bundleOperator),
                                oneInputOperator,
                                oneInputOperator.getProducedType(),
                                root.getParallelism());
            } else {
                TwoInputTransformation<?, ?, ?> twoInputTransformation =
                        (TwoInputTransformation<?, ?, ?>) root;
                Transformation<?> leftTransform = twoInputTransformation.getInput1();
                Transformation<?> rightTransform = twoInputTransformation.getInput2();
                MultiOutputPythonProcessOperator.TwoInputOperator<?, ?> twoInputOperator =
                        bundleOperator.getTwoInputOperator(
                                leftTransform.getOutputType(), rightTransform.getOutputType());
                bundleTransformation =
                        new TwoInputTransformation(
                                leftTransform,
                                rightTransform,
                                generateName(bundleOperator),
                                twoInputOperator,
                                twoInputOperator.getProducedType(),
                                root.getParallelism());
            }

            return bundleTransformation;
        }

        private void calibrateBundleTransformation() {
            Preconditions.checkNotNull(bundleTransformation);
            if (transformations.size() == 1) {
                return;
            }
            copyInput(rootTransformation.getTransformation(), bundleTransformation);
        }

        private String generateName(MultiOutputPythonProcessOperator operator) {
            String[] strings = new String[transformations.size()];
            int index = 0;

            Set<TransformationWrapper> visited = new HashSet<>();
            Queue<TransformationWrapper> queue = new ArrayDeque<>();
            queue.add(rootTransformation);
            visited.add(rootTransformation);
            while (!queue.isEmpty()) {
                TransformationWrapper transformation = queue.poll();
                strings[index++] = transformation.getTransformation().getName();
                for (TransformationWrapper successor :
                        transformationGraph.getSuccessors(transformation)) {
                    if (transformations.contains(successor) && !visited.contains(successor)) {
                        visited.add(successor);
                        queue.add(successor);
                    }
                }
            }
            return "Op: "
                    + String.join(",", strings)
                    + ". Out: "
                    + operator.getOutputTags().stream()
                            .map(OutputTag::toString)
                            .collect(Collectors.joining(","));
        }
    }

    private static class TransformationGraph {

        private final Set<TransformationWrapper> sources;
        private final Map<TransformationWrapper, Set<TransformationWrapper>> predecessorMap;
        private final Map<TransformationWrapper, Set<TransformationWrapper>> successorMap;
        private final Map<Transformation<?>, TransformationWrapper> transformationWrapperMap;

        private TransformationGraph(Collection<Transformation<?>> transformations) {
            Preconditions.checkNotNull(transformations);
            sources = Sets.newIdentityHashSet();
            predecessorMap = Maps.newIdentityHashMap();
            successorMap = Maps.newIdentityHashMap();
            transformationWrapperMap = Maps.newIdentityHashMap();
            buildGraph(transformations);
        }

        private Set<TransformationWrapper> getSources() {
            return ImmutableSet.copyOf(sources);
        }

        private Set<TransformationWrapper> getPredecessors(TransformationWrapper transformation) {
            return ImmutableSet.copyOf(
                    predecessorMap.computeIfAbsent(transformation, i -> Sets.newIdentityHashSet()));
        }

        private Set<TransformationWrapper> getPredecessors(
                Collection<TransformationWrapper> transformations) {
            Set<TransformationWrapper> result = Sets.newIdentityHashSet();
            for (TransformationWrapper transformation : transformations) {
                result.addAll(
                        predecessorMap.computeIfAbsent(
                                transformation, i -> Sets.newIdentityHashSet()));
            }
            result.removeAll(transformations);
            return result;
        }

        private Set<TransformationWrapper> getSuccessors(TransformationWrapper transformation) {
            return ImmutableSet.copyOf(
                    successorMap.computeIfAbsent(transformation, i -> Sets.newIdentityHashSet()));
        }

        private Set<TransformationWrapper> getSuccessors(
                Collection<TransformationWrapper> transformations) {
            Set<TransformationWrapper> result = Sets.newIdentityHashSet();
            for (TransformationWrapper transformation : transformations) {
                result.addAll(
                        successorMap.computeIfAbsent(
                                transformation, i -> Sets.newIdentityHashSet()));
            }
            result.removeAll(transformations);
            return result;
        }

        private void insertIntermediateTransformation(
                Transformation<?> intermediate,
                TransformationWrapper upTransform,
                TransformationWrapper downTransform) {
            Preconditions.checkArgument(successorMap.containsKey(upTransform));
            Preconditions.checkArgument(predecessorMap.containsKey(downTransform));

            TransformationWrapper wrapper =
                    transformationWrapperMap.computeIfAbsent(
                            intermediate, i -> new TransformationWrapper(intermediate));

            Set<TransformationWrapper> predecessors = predecessorMap.get(downTransform);
            predecessors.remove(upTransform);
            predecessors.add(wrapper);

            Set<TransformationWrapper> successors = successorMap.get(upTransform);
            successors.remove(downTransform);
            successors.add(wrapper);
        }

        private void buildGraph(Collection<Transformation<?>> transformations) {
            Set<Transformation<?>> visitedTransformation = Sets.newIdentityHashSet();
            Queue<Transformation<?>> queue = new ArrayDeque<>(transformations);
            while (!queue.isEmpty()) {
                Transformation<?> transformation = queue.poll();
                if (visitedTransformation.contains(transformation)) {
                    continue;
                }
                visitedTransformation.add(transformation);
                TransformationWrapper wrapper =
                        transformationWrapperMap.computeIfAbsent(
                                transformation, i -> new TransformationWrapper(transformation));

                if (transformation.getInputs().isEmpty()
                        && (transformation instanceof SourceTransformation
                                || transformation instanceof LegacySourceTransformation)) {
                    sources.add(wrapper);
                }

                Set<TransformationWrapper> predecessors =
                        predecessorMap.computeIfAbsent(wrapper, i -> Sets.newIdentityHashSet());

                for (Transformation<?> input : transformation.getInputs()) {
                    TransformationWrapper inputWrapper =
                            transformationWrapperMap.computeIfAbsent(
                                    input, i -> new TransformationWrapper(input));
                    Set<TransformationWrapper> successors =
                            successorMap.computeIfAbsent(
                                    inputWrapper, i -> Sets.newIdentityHashSet());
                    successors.add(wrapper);
                    predecessors.add(inputWrapper);
                }

                if (transformation instanceof SideOutputTransformation) {
                    SideOutputTransformation<?> sideOutputTransformation =
                            (SideOutputTransformation<?>) transformation;
                    Preconditions.checkArgument(
                            sideOutputTransformation.getInputs().size() == 1,
                            "SideOutputTransformation must have only one predecessor");
                    TransformationWrapper inputWrapper =
                            transformationWrapperMap.get(
                                    sideOutputTransformation.getInputs().get(0));
                    inputWrapper.setFork();
                    wrapper.setForkTag(sideOutputTransformation.getOutputTag().getId());
                }

                queue.addAll(transformation.getInputs());
            }
        }
    }

    private static class TransformationWrapper {

        private final Transformation<?> transformation;
        private boolean fork = false;
        private String forkTag = null;
        private TransitiveClosure closure = null;

        private TransformationWrapper(Transformation<?> transformation) {
            this.transformation = Preconditions.checkNotNull(transformation);
        }

        private Transformation<?> getTransformation() {
            return transformation;
        }

        private void setFork() {
            fork = true;
        }

        private boolean isFork() {
            return fork;
        }

        private void setForkTag(String tag) {
            forkTag = tag;
        }

        private String getForkTag() {
            return forkTag;
        }

        private void setClosure(TransitiveClosure closure) {
            this.closure = closure;
        }

        private TransitiveClosure getClosure() {
            return closure;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            TransformationWrapper wrapper = (TransformationWrapper) obj;
            if (wrapper.getTransformation() == null || getTransformation() == null) {
                return false;
            }
            return getTransformation().equals(wrapper.getTransformation());
        }
    }

    private static void copyInput(Transformation<?> src, Transformation<?> dest) {
        Preconditions.checkArgument(src.getClass().equals(dest.getClass()));
        try {
            if (src instanceof OneInputTransformation
                    || src instanceof FeedbackTransformation
                    || src instanceof SideOutputTransformation
                    || src instanceof ReduceTransformation
                    || src instanceof SinkTransformation
                    || src instanceof LegacySinkTransformation
                    || src instanceof TimestampsAndWatermarksTransformation
                    || src instanceof PartitionTransformation) {
                final Field inputField = src.getClass().getDeclaredField("input");
                inputField.setAccessible(true);
                inputField.set(dest, inputField.get(src));
            } else if (src instanceof TwoInputTransformation) {
                final Field input1Field = src.getClass().getDeclaredField("input1");
                final Field input2Field = src.getClass().getDeclaredField("input2");
                input1Field.setAccessible(true);
                input1Field.set(dest, input1Field.get(src));
                input2Field.setAccessible(true);
                input2Field.set(dest, input2Field.get(src));
            } else if (src instanceof UnionTransformation
                    || src instanceof AbstractMultipleInputTransformation) {
                final Field inputsField = src.getClass().getDeclaredField("inputs");
                inputsField.setAccessible(true);
                List<Transformation<?>> newInputs = Lists.newArrayList(src.getInputs());
                inputsField.set(dest, newInputs);
            } else if (src instanceof AbstractBroadcastStateTransformation) {
                final Field regularInputField = src.getClass().getDeclaredField("regularInput");
                final Field broadcastInputField = src.getClass().getDeclaredField("broadcastInput");
                regularInputField.setAccessible(true);
                regularInputField.set(dest, regularInputField.get(src));
                broadcastInputField.setAccessible(true);
                broadcastInputField.set(dest, broadcastInputField.get(src));
            } else {
                throw new RuntimeException(
                        "Unsupported transformation: " + src.getClass().getSimpleName());
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // This should never happen
            throw new RuntimeException(e);
        }
    }
}
