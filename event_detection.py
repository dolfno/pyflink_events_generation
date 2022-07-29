"""going strong
TODO:
* keep event id and store with values
* to check, also add counter by default.
"""
from sqlite3 import Timestamp
from pyflink.common.typeinfo import Types

from pyflink.datastream.functions import RuntimeContext, FlatMapFunction
from pyflink.datastream.state import ValueStateDescriptor

import logging

from processing.processors import ValueProcessor, BUILDIN_FUNCTIONS
from flink_types.processed_measurements import ProcessedMeasurements

MAX_VALUE_SLOTS = 10

pm = ProcessedMeasurements()


class EventDetection(FlatMapFunction):
    def open(self, runtime_context: RuntimeContext):
        self.previous_event_state = runtime_context.get_state(
            ValueStateDescriptor("previous_event_state", Types.PICKLED_BYTE_ARRAY())
        )
        for i in range(MAX_VALUE_SLOTS):
            slot_name = f"value_slot_{i}"
            self.__setattr__(
                slot_name, runtime_context.get_state(ValueStateDescriptor(slot_name, Types.PICKLED_BYTE_ARRAY()))
            )

    def flat_map(self, measurement):
        results = []
        if measurement.category == "event_type":
            results = self.process_event(measurement)
        elif measurement.category == "value_type":
            results = self.process_value(measurement)
        else:
            raise ValueError(f"Unknown category: {measurement.category}")

        for result in results:
            yield result

    def process_event(self, measurement):
        previous_event_state = self.previous_event_state.value()

        events = []
        if previous_event_state:
            closed_event = pm.create_row(
                measurement.category,
                measurement.group_id,
                previous_event_state.v,
                previous_event_state.t,
                measurement.t,
            )
            events.append(closed_event)

        new_event = pm.create_row(
            measurement.category,
            measurement.group_id,
            measurement.v,
            measurement.t,
            None,
        )
        events.append(new_event)
        logging.warning("previous event {}".format(previous_event_state))

        closed_values = self.close_values(measurement)

        # Finally update state
        self.previous_event_state.update(measurement)

        return [*events, *closed_values]

    def process_value(self, measurement):
        value_result = []
        value_state = self.__getattribute__(f"value_slot_{measurement.slot}").value()

        logging.warning("print value state  before if {}".format(value_state))
        logging.warning("measuremnet {}".format(measurement))

        if isinstance(value_state, ValueProcessor):
            logging.warning("print value state {}".format(value_state))
            logging.warning("print value state values {}".format(value_state.values))
            value_state.values.append(measurement.v)
            value_state.timestamps.append(int(measurement.t))

            if len(value_state.values) > 1:
                value_result.append(
                    pm.create_row(
                        measurement.category,
                        measurement.group_id,
                        value_state.aggregate,
                        measurement.t,
                        None,
                        measurement.value_config_id,
                        self.current_event_id,
                        value_state.value_count,
                    )
                )
        else:
            value_state = ValueProcessor(
                [measurement.v],
                [int(measurement.t)],
                measurement.value_config_id,
                measurement.method,
            )
            logging.warning("print value state else {}".format(value_state))

        self.__getattribute__(f"value_slot_{measurement.slot}").update(value_state)
        logging.warning("print get att {}".format(self.__getattribute__(f"value_slot_{measurement.slot}")))

        return value_result

    def close_values(self, event_measurement):
        """When the event is reset, you need to keep the first value incl. ts of the event start."""
        closed_values = []
        for i in range(1):
            value_state: ValueProcessor = self.__getattribute__(f"value_slot_{i}").value()
            if value_state:
                value_state.append_measurement(value=None, timestamp=event_measurement.t)
                closed_values.append(
                    pm.create_row(
                        "value_type",
                        event_measurement.group_id,
                        value_state.aggregate,
                        event_measurement.t,
                        None,
                        value_state.value_config_id,
                        self.current_event_id,
                        value_state.value_count,
                    )
                )

                logging.warning("valuestate in {}".format(value_state))
                value_state = value_state.clear()
                self.__getattribute__(f"value_slot_{i}").update(value_state)
                logging.warning("valuestate out {}".format(value_state))
        return closed_values

    @property
    def current_event_id(self):
        previous_event = self.previous_event_state.value()
        if previous_event:
            return f"{previous_event.group_id}_{previous_event.t}"
        else:
            return "Unknown"
