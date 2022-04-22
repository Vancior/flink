################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
# cython: language_level = 3
# cython: infer_types = True
# cython: profile=True
# cython: boundscheck=False, wraparound=False, initializedcheck=False, cdivision=True
from libc.stdint cimport *

from apache_beam.coders.coder_impl cimport OutputStream as BOutputStream
from apache_beam.runners.worker.bundle_processor import DataOutputOperation
from apache_beam.utils cimport windowed_value
from apache_beam.utils.windowed_value cimport WindowedValue

from pyflink.common.constants import DEFAULT_OUTPUT_TAG
from pyflink.fn_execution.coder_impl_fast cimport InputStreamWrapper
from pyflink.fn_execution.table.operations import BundleOperation, BaseOperation as TableOperation
from pyflink.fn_execution.profiler import Profiler


cdef class InputProcessor:

    cpdef has_next(self):
        pass

    cpdef next(self):
        pass


cdef class NetworkInputProcessor(InputProcessor):

    def __init__(self, InputStreamWrapper input_stream_wrapper):
        self._input_stream_wrapper = input_stream_wrapper

    cpdef has_next(self):
        return self._input_stream_wrapper.has_next()

    cpdef next(self):
        return self._input_stream_wrapper.next()


cdef class IntermediateInputProcessor(InputProcessor):

    def __init__(self, input_values):
        if isinstance(input_values, list):
            self._input_values = iter(input_values)
        else:
            self._input_values = input_values
        self._next_value = None

    cpdef has_next(self):
        try:
            self._next_value = next(self._input_values)
        except StopIteration:
            self._next_value = None

        return self._next_value is not None

    cpdef next(self):
        return self._next_value


cdef class OutputProcessor:

    cpdef process_outputs(self, WindowedValue windowed_value, results):
        pass

    cpdef close(self):
        pass

cdef class NetworkOutputProcessor(OutputProcessor):

    def __init__(self, consumer):
        assert isinstance(consumer, DataOutputOperation)
        self._consumer = consumer
        self._value_coder_impl = consumer.windowed_coder.wrapped_value_coder.get_impl()._value_coder

    cpdef process_outputs(self, WindowedValue windowed_value, results):
        output_stream = self._consumer.output_stream
        self._value_coder_impl.encode_to_stream(results, output_stream, True)
        self._value_coder_impl._output_stream.maybe_flush()

    cpdef close(self):
        self._value_coder_impl._output_stream.close()

cdef class IntermediateOutputProcessor(OutputProcessor):

    def __init__(self, consumer):
        self._consumer = consumer

    cpdef process_outputs(self, WindowedValue windowed_value, results):
        self._consumer.process(windowed_value.with_value(results))


cdef class FunctionOperation(Operation):
    """
    Base class of function operation that will execute StatelessFunction or StatefulFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls):
        super(FunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self._output_processors = FunctionOperation._create_output_processors(consumers)  \
            # type: Dict[str, List[OutputProcessor]]
        self.operation_cls = operation_cls
        self.operation = self.generate_operation()
        self.process_element = self.operation.process_element
        self.operation.open()
        if spec.serialized_fn.profile_enabled:
            self._profiler = Profiler()
        else:
            self._profiler = None
        job_parameters = {p.key: p.value for p in spec.serialized_fn.runtime_context.job_parameters}
        if job_parameters.get("SIDE_OUTPUT_ENABLED") is not None:
            self._side_output_enabled = True
        else:
            self._side_output_enabled = False
            self._only_processor = self._output_processors[DEFAULT_OUTPUT_TAG][0]

    cpdef start(self):
        with self.scoped_start_state:
            super(FunctionOperation, self).start()
            if self._profiler:
                self._profiler.start()

    cpdef finish(self):
        with self.scoped_finish_state:
            super(FunctionOperation, self).finish()
            self.operation.finish()
            if self._profiler:
                self._profiler.close()

    cpdef teardown(self):
        with self.scoped_finish_state:
            self.operation.close()
            for ps in self._output_processors.values():
                for p in ps:
                    p.close()

    cpdef process(self, WindowedValue o):
        cdef InputStreamWrapper input_stream_wrapper
        cdef InputProcessor input_processor
        with self.scoped_process_state:
            if isinstance(o.value, InputStreamWrapper):
                input_processor = NetworkInputProcessor(o.value)
            else:
                input_processor = IntermediateInputProcessor(o.value)
            if isinstance(self.operation, BundleOperation):
                while input_processor.has_next():
                    self.process_element(input_processor.next())
                self._only_processor.process_outputs(o, self.operation.finish_bundle())
            elif isinstance(self.operation, TableOperation):
                while input_processor.has_next():
                    self._only_processor.process_outputs(
                        o,
                        self.process_element(input_processor.next())
                    )
            else:
                if self._side_output_enabled:
                    while input_processor.has_next():
                        result = self.process_element(input_processor.next())
                        for tag, row in result:
                            ps = self._output_processors.get(tag)
                            if ps is not None:
                                for p in ps:
                                    (<OutputProcessor> p).process_outputs(o, [row])
                else:
                    while input_processor.has_next():
                        self._only_processor.process_outputs(
                            o,
                            self.process_element(input_processor.next())
                        )

    def progress_metrics(self):
        metrics = super(FunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    cpdef monitoring_infos(self, transform_id, tag_to_pcollection_id):
        """
        Only pass user metric to Java
        :param tag_to_pcollection_id: useless for user metric
        """
        return self.user_monitoring_infos(transform_id)

    @staticmethod
    def _create_output_processors(consumers_map):
        def _create_processor(consumer):
            if isinstance(consumer, DataOutputOperation):
                return NetworkOutputProcessor(consumer)
            else:
                return IntermediateOutputProcessor(consumer)

        return {tag: [_create_processor(c) for c in consumers] for tag, consumers in
                consumers_map.items()}


    cdef object generate_operation(self):
        pass


cdef class StatelessFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls):
        super(StatelessFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls)

    cdef object generate_operation(self):
        return self.operation_cls(self.spec.serialized_fn)


cdef class StatefulFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls,
                 keyed_state_backend):
        self._keyed_state_backend = keyed_state_backend
        self._reusable_windowed_value = windowed_value.create(None, -1, None, None)
        super(StatefulFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls)

    cdef object generate_operation(self):
        return self.operation_cls(self.spec.serialized_fn, self._keyed_state_backend)

    cpdef void add_timer_info(self, timer_family_id, timer_info):
        # ignore timer_family_id
        self.operation.add_timer_info(timer_info)

    cpdef process_timer(self, tag, timer_data):
        cdef BOutputStream output_stream
        if self._side_output_enabled:
            for tag, row in self.operation.process_timer(timer_data.user_key):
                ps = self._output_processors.get(tag)
                if ps is not None:
                    for p in ps:
                        (<OutputProcessor> p).process_outputs(self._reusable_windowed_value, [row])
        else:
            self._only_processor.process_outputs(self._reusable_windowed_value,
                                                 self.operation.process_timer(timer_data.user_key))
