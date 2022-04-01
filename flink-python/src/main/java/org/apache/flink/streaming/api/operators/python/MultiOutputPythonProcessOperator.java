package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.functions.python.DagDataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.functions.python.DefaultDataStreamPythonFunctionInfo;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataStream Python function operator that can emit multiple outputs with corresponding {@link
 * OutputTag}. {@link Object} type is used as a dummy type for the generic type of {@link
 * org.apache.flink.streaming.api.operators.StreamOperator}.
 */
@Internal
public class MultiOutputPythonProcessOperator
        extends AbstractExternalPythonFunctionOperator<Object> {

    private DagDataStreamPythonFunctionInfo dagDataStreamPythonFunctionInfo;

    private final List<OutputTag<?>> outputTagList;

    /** For type conflict check when adding output tags, never used in runtime. */
    private final transient Map<OutputTag<?>, TypeInformation<?>> outputTagTypeMap;

    /**
     * For {@link OutputTag} lookup in runtime, each output {@link
     * org.apache.beam.model.pipeline.v1.RunnerApi.PCollection} is associated with a {@link
     * OutputTag} with the same id. This field should be initialized in {@code open()}.
     */
    private transient Map<String, OutputTag<?>> outputTagIdMap;

    public MultiOutputPythonProcessOperator(
            Configuration config, DagDataStreamPythonFunctionInfo dagDataStreamPythonFunctionInfo) {
        super(config);
        this.dagDataStreamPythonFunctionInfo = dagDataStreamPythonFunctionInfo;
        outputTagList = new ArrayList<>();
        outputTagTypeMap = new HashMap<>();
        outputTagIdMap = new HashMap<>();
    }

    @Override
    public void open() throws Exception {
        outputTagIdMap = new HashMap<>();
        for (OutputTag<?> tag : outputTagList) {
            outputTagIdMap.put(tag.getId(), tag);
        }
        super.open();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {}

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return null;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return null;
    }

    public void addOutputTag(OutputTag<?> tag) {
        TypeInformation<?> typeInformation = outputTagTypeMap.get(tag);
        if (typeInformation != null && !typeInformation.equals(tag.getTypeInfo())) {
            throw new IllegalArgumentException(
                    "Conflict OutputTags with same id but different types");
        }
        outputTagList.add(tag);
        outputTagTypeMap.put(tag, tag.getTypeInfo());
    }

    public List<OutputTag<?>> getOutputTags() {
        return ImmutableList.copyOf(outputTagList);
    }

    public <IN> OneInputOperator<IN> getOneInputOperator(TypeInformation<IN> inputTypeInfo) {
        return new OneInputOperator<>(inputTypeInfo);
    }

    public <IN1, IN2> TwoInputOperator<IN1, IN2> getTwoInputOperator(
            TypeInformation<IN1> inputTypeInfo1, TypeInformation<IN2> inputTypeInfo2) {
        return new TwoInputOperator<>(inputTypeInfo1, inputTypeInfo2);
    }

    /**
     * This operator does not have main output, currently dummy output type {@link Byte} is used.
     */
    public class OneInputOperator<IN> extends AbstractOneInputPythonFunctionOperator<IN, Byte> {

        public OneInputOperator(TypeInformation<IN> inputTypeInfo) {
            super(
                    MultiOutputPythonProcessOperator.this.getConfiguration(),
                    createDummyPythonFunctionInfo(),
                    inputTypeInfo,
                    TypeInformation.of(Byte.class));
        }

        @Override
        public <T> AbstractDataStreamPythonFunctionOperator<T> copy(
                DefaultDataStreamPythonFunctionInfo pythonFunctionInfo,
                TypeInformation<T> outputTypeInfo) {
            throw new UnsupportedOperationException(
                    "Bundled MultiOutputDataStreamPythonFunctionOperator cannot be copied");
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
            return MultiOutputPythonProcessOperator.this.createPythonFunctionRunner();
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {}
    }

    /**
     * This operator does not have main output, currently dummy output type {@link Byte} is used.
     */
    public class TwoInputOperator<IN1, IN2>
            extends AbstractTwoInputPythonFunctionOperator<IN1, IN2, Byte> {

        public TwoInputOperator(
                TypeInformation<IN1> inputTypeInfo1, TypeInformation<IN2> inputTypeInfo2) {
            super(
                    MultiOutputPythonProcessOperator.this.getConfiguration(),
                    createDummyPythonFunctionInfo(),
                    inputTypeInfo1,
                    inputTypeInfo2,
                    TypeInformation.of(Byte.class));
        }

        @Override
        public <T> AbstractDataStreamPythonFunctionOperator<T> copy(
                DefaultDataStreamPythonFunctionInfo pythonFunctionInfo,
                TypeInformation<T> outputTypeInfo) {
            throw new UnsupportedOperationException(
                    "Bundled MultiOutputDataStreamPythonFunctionOperator cannot be copied");
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
            return MultiOutputPythonProcessOperator.this.createPythonFunctionRunner();
        }

        @Override
        public void processElement1(StreamRecord<IN1> element) throws Exception {}

        @Override
        public void processElement2(StreamRecord<IN2> element) throws Exception {}
    }

    private static DefaultDataStreamPythonFunctionInfo createDummyPythonFunctionInfo() {
        return new DefaultDataStreamPythonFunctionInfo(
                new PythonFunction() {
                    @Override
                    public byte[] getSerializedPythonFunction() {
                        return new byte[0];
                    }

                    @Override
                    public PythonEnv getPythonEnv() {
                        return null;
                    }
                },
                0);
    }
}
