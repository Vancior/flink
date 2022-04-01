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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.PythonCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonProcessOperator;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.util.OutputTag;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.python.chain.PythonOperatorChainingOptimizerTest.createCoKeyedProcessOperator;
import static org.apache.flink.python.chain.PythonOperatorChainingOptimizerTest.createCoProcessOperator;
import static org.apache.flink.python.chain.PythonOperatorChainingOptimizerTest.createKeyedProcessOperator;
import static org.apache.flink.python.chain.PythonOperatorChainingOptimizerTest.createProcessOperator;
import static org.mockito.Mockito.mock;

/** Tests for {@link PythonOperatorTransitiveChainingOptimizer}. */
public class PythonOperatorTransitiveChainingOptimizerTest {

    @Test
    public void testOptimizeSingleChain() {
        PythonProcessOperator<?, ?> processOperator0 =
                createProcessOperator("f0", Types.INT(), Types.INT());
        PythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator(
                        "_stream_key_by_map_operator",
                        Types.INT(),
                        new RowTypeInfo(Types.INT(), Types.INT()));
        PythonKeyedProcessOperator<?> keyedProcessOperator =
                createKeyedProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        PythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f2", Types.STRING(), Types.LONG());
        PythonProcessOperator<?, ?> processOperator3 =
                createProcessOperator("f3", Types.LONG(), Types.INT());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        Transformation<?> processTransformation0 =
                new OneInputTransformation(
                        sourceTransformation,
                        "process0",
                        processOperator0,
                        processOperator0.getProducedType(),
                        2);
        Transformation<?> addKeyTransformation =
                new OneInputTransformation(
                        processTransformation0,
                        "_stream_key_by_map_opreator",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> partitionTransformation =
                new PartitionTransformation(
                        addKeyTransformation, mock(KeyGroupStreamPartitioner.class));
        Transformation<?> keyedProcessTransformation =
                new OneInputTransformation(
                        partitionTransformation,
                        "keyedProcess",
                        keyedProcessOperator,
                        keyedProcessOperator.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        keyedProcessTransformation,
                        "process1",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process2",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(addKeyTransformation);
        transformations.add(keyedProcessTransformation);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);

        PythonOperatorTransitiveChainingOptimizer.optimize(transformations);
    }

    @Test
    public void testOptimizeForking() {
        PythonProcessOperator<?, ?> processOperator0 =
                createProcessOperator("f0", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator("f1", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f2", Types.LONG(), Types.LONG());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        Mockito.when(sourceTransformation.getName()).thenReturn("source");
        sourceTransformation.setName("source");
        Transformation<?> processTransformation0 =
                new OneInputTransformation(
                        sourceTransformation,
                        "process0",
                        processOperator0,
                        processOperator0.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        processTransformation0,
                        "process1",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> sideOutputTransformation =
                new SideOutputTransformation(
                        processTransformation0, new OutputTag<Long>("side") {});
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        sideOutputTransformation,
                        "process2",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);
        Transformation<?> sinkTransformation =
                new LegacySinkTransformation(
                        processTransformation1, "sink", mock(StreamSink.class), 2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(processTransformation0);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);
        transformations.add(sinkTransformation);

        PythonOperatorTransitiveChainingOptimizer.optimize(transformations);
    }

    @Test
    public void testOptimizeMultiSourceMerging() {
        PythonProcessOperator<?, ?> op10 =
                createProcessOperator("f1-0", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op20 =
                createProcessOperator("f2-0", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op21 =
                createProcessOperator("f2-1", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op22 =
                createProcessOperator("f2-2", Types.STRING(), Types.STRING());
        PythonKeyedCoProcessOperator<?> kop3 =
                createCoKeyedProcessOperator(
                        "f3",
                        new RowTypeInfo(Types.STRING()),
                        new RowTypeInfo(Types.STRING()),
                        Types.STRING());

        Transformation<?> source1 = mock(SourceTransformation.class);
        Mockito.when(source1.getName()).thenReturn("source1");
        Transformation<?> source2 = mock(SourceTransformation.class);
        Mockito.when(source2.getName()).thenReturn("source2");
        Transformation<?> transform10 =
                new OneInputTransformation(source1, "op1-0", op10, op10.getProducedType(), 1);
        Transformation<?> transform20 =
                new OneInputTransformation(source2, "op2-0", op20, op20.getProducedType(), 1);
        Transformation<?> transform21 =
                new OneInputTransformation(transform20, "op2-1", op21, op21.getProducedType(), 1);
        Transformation<?> transform22 =
                new OneInputTransformation(transform21, "op2-2", op22, op22.getProducedType(), 1);
        Transformation<?> partition1 =
                new PartitionTransformation(transform10, mock(KeyGroupStreamPartitioner.class));
        Transformation<?> partition2 =
                new PartitionTransformation(transform22, mock(KeyGroupStreamPartitioner.class));
        Transformation<?> merge =
                new TwoInputTransformation(
                        partition1, partition2, "kop3", kop3, kop3.getProducedType(), 1);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(source1);
        transformations.add(source2);
        transformations.add(transform10);
        transformations.add(transform20);
        transformations.add(transform21);
        transformations.add(transform22);
        transformations.add(merge);

        PythonOperatorTransitiveChainingOptimizer.optimize(transformations);
    }

    @Test
    public void testOptimizeForkingAndMerging() {
        PythonProcessOperator<?, ?> op0 =
                createProcessOperator("op0", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op10 =
                createProcessOperator("f1-0", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op20 =
                createProcessOperator("f2-0", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op21 =
                createProcessOperator("f2-1", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op22 =
                createProcessOperator("f2-2", Types.STRING(), Types.STRING());
        PythonCoProcessOperator<?, ?, ?> op3 =
                createCoProcessOperator("f3", Types.STRING(), Types.STRING(), Types.STRING());

        Transformation<?> source = mock(SourceTransformation.class);
        Mockito.when(source.getName()).thenReturn("source");
        Transformation<?> transform0 =
                new OneInputTransformation(source, "op0", op0, op0.getProducedType(), 1);
        Transformation<?> transform10 =
                new OneInputTransformation(transform0, "op1-0", op10, op10.getProducedType(), 1);
        Transformation<?> sideOutput =
                new SideOutputTransformation(transform0, new OutputTag<Long>("side") {});
        Transformation<?> transform20 =
                new OneInputTransformation(sideOutput, "op2-0", op20, op20.getProducedType(), 1);
        Transformation<?> transform21 =
                new OneInputTransformation(transform20, "op2-1", op21, op21.getProducedType(), 1);
        Transformation<?> transform22 =
                new OneInputTransformation(transform21, "op2-2", op22, op22.getProducedType(), 1);
        Transformation<?> merge =
                new TwoInputTransformation(
                        transform10, transform22, "op3", op3, op3.getProducedType(), 1);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(source);
        transformations.add(transform10);
        transformations.add(transform20);
        transformations.add(transform21);
        transformations.add(transform22);
        transformations.add(merge);

        PythonOperatorTransitiveChainingOptimizer.optimize(transformations);
    }

    @Test
    public void testOptimizeForkingAndNotMerging() {
        PythonProcessOperator<?, ?> op1 =
                createProcessOperator("f1", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op2 =
                createProcessOperator("f2", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op3 =
                createProcessOperator("f3", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op4 =
                createProcessOperator("f4", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op5 =
                createProcessOperator("f5", Types.STRING(), Types.STRING());

        Transformation<?> source =
                new SourceTransformation(
                        "source",
                        mock(Source.class),
                        mock(WatermarkStrategy.class),
                        Types.STRING(),
                        1);
        Transformation<?> transform1 =
                new OneInputTransformation(source, "py_op1", op1, op1.getProducedType(), 1);
        Transformation<?> transform2 =
                new OneInputTransformation(transform1, "py_op2", op2, op2.getProducedType(), 1);
        Transformation<?> transform3 =
                new OneInputTransformation(transform2, "py_op3", op3, op3.getProducedType(), 1);
        Transformation<?> sideOutput =
                new SideOutputTransformation(transform1, new OutputTag<Long>("side") {});
        Transformation<?> transform4 =
                new OneInputTransformation(sideOutput, "py_op4", op4, op4.getProducedType(), 1);
        Transformation<?> transform5 =
                new OneInputTransformation(transform4, "py_op5", op5, op5.getProducedType(), 1);
        Transformation<?> notMerge =
                new TwoInputTransformation(
                        transform3,
                        transform5,
                        "java_merge",
                        mock(TwoInputStreamOperator.class),
                        Types.STRING(),
                        1);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(source);
        transformations.add(transform1);
        transformations.add(transform2);
        transformations.add(transform3);
        transformations.add(transform4);
        transformations.add(transform5);
        transformations.add(notMerge);

        for (Transformation<?> transformation :
                PythonOperatorTransitiveChainingOptimizer.optimize(transformations)) {
            System.out.println(transformation == null ? "null" : transformation.getName());
        }
    }

    /**
     * test1.
     *
     * <p>source -> java_op1 -> py_op1 -> py_op2 ----------> java_merge
     *
     * <p>\-> side -> py_op3 -> py_op4 /
     */
    @Test
    public void test1() {
        PythonProcessOperator<?, ?> op1 =
                createProcessOperator("f1", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op2 =
                createProcessOperator("f2", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op3 =
                createProcessOperator("f3", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op4 =
                createProcessOperator("f4", Types.STRING(), Types.STRING());

        Transformation<?> source = mock(SourceTransformation.class);
        Mockito.when(source.getName()).thenReturn("source");
        Transformation<?> jop1 =
                new OneInputTransformation(
                        source, "jop1", mock(OneInputStreamOperator.class), Types.STRING(), 1);
        Transformation<?> pop1 = new OneInputTransformation(jop1, "pop1", op1, Types.STRING(), 1);
        Transformation<?> pop2 = new OneInputTransformation(pop1, "pop2", op2, Types.STRING(), 1);
        Transformation<?> side =
                new SideOutputTransformation(jop1, new OutputTag<String>("side") {});
        Transformation<?> pop3 = new OneInputTransformation(side, "pop3", op3, Types.STRING(), 1);
        Transformation<?> pop4 = new OneInputTransformation(pop3, "pop4", op4, Types.STRING(), 1);
        Transformation<?> jMerge =
                new TwoInputTransformation(
                        pop2,
                        pop4,
                        "j-merge",
                        mock(TwoInputStreamOperator.class),
                        Types.STRING(),
                        1);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(source);
        transformations.add(jop1);
        transformations.add(pop1);
        transformations.add(pop2);
        transformations.add(pop3);
        transformations.add(pop4);
        transformations.add(jMerge);

        for (Transformation<?> transformation :
                PythonOperatorTransitiveChainingOptimizer.optimize(transformations)) {
            System.out.println(transformation == null ? "null" : transformation.getName());
        }
    }

    /**
     * test2.
     *
     * <p>source -> java_op1 -> java_op2 -> py_op1 ----------> java_merge
     *
     * <p>\-> side -> py_op2 -> py_op3 /
     */
    @Test
    public void test2() {
        PythonProcessOperator<?, ?> op1 =
                createProcessOperator("f1", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op2 =
                createProcessOperator("f2", Types.STRING(), Types.STRING());
        PythonProcessOperator<?, ?> op3 =
                createProcessOperator("f3", Types.STRING(), Types.STRING());

        Transformation<?> source = mock(SourceTransformation.class);
        Mockito.when(source.getName()).thenReturn("source");
        Transformation<?> jop1 =
                new OneInputTransformation(
                        source, "jop1", mock(OneInputStreamOperator.class), Types.STRING(), 1);
        Transformation<?> jop2 =
                new OneInputTransformation(
                        jop1, "jop2", mock(OneInputStreamOperator.class), Types.STRING(), 1);
        Transformation<?> pop1 = new OneInputTransformation(jop2, "pop1", op1, Types.STRING(), 1);
        Transformation<?> side =
                new SideOutputTransformation(jop1, new OutputTag<String>("side") {});
        Transformation<?> pop2 = new OneInputTransformation(side, "pop2", op2, Types.STRING(), 1);
        Transformation<?> pop3 = new OneInputTransformation(pop2, "pop3", op3, Types.STRING(), 1);
        Transformation<?> jMerge =
                new TwoInputTransformation(
                        pop1,
                        pop3,
                        "j-merge",
                        mock(TwoInputStreamOperator.class),
                        Types.STRING(),
                        1);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(source);
        transformations.add(jop1);
        transformations.add(jop2);
        transformations.add(pop1);
        transformations.add(pop2);
        transformations.add(pop3);
        transformations.add(jMerge);

        for (Transformation<?> transformation :
                PythonOperatorTransitiveChainingOptimizer.optimize(transformations)) {
            System.out.println(transformation == null ? "null" : transformation.getName());
        }
    }
}
