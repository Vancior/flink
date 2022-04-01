package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.python.timer.TimerRegistration;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.TimerReference;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** */
public class BeamDagDataStreamPythonFunctionRunner extends BeamPythonFunctionRunner {
    public BeamDagDataStreamPythonFunctionRunner(
            String taskName,
            ProcessPythonEnvironmentManager environmentManager,
            Map<String, String> jobOptions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            @Nullable KeyedStateBackend keyedStateBackend,
            @Nullable TypeSerializer keySerializer,
            @Nullable TypeSerializer namespaceSerializer,
            @Nullable TimerRegistration timerRegistration,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor) {
        super(
                taskName,
                environmentManager,
                jobOptions,
                flinkMetricContainer,
                keyedStateBackend,
                keySerializer,
                namespaceSerializer,
                timerRegistration,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor);
    }

    @Override
    protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {}

    @Override
    protected List<TimerReference> getTimers(RunnerApi.Components components) {
        return null;
    }

    @Override
    protected Optional<RunnerApi.Coder> getOptionalTimerCoderProto() {
        return Optional.empty();
    }
}
