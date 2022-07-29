from dataclasses import dataclass
from typing import Any, Callable, List
import numpy as np
import logging

BUILDIN_FUNCTIONS = {
    "mean": lambda v, d: float(sum(d * np.array(v[:-1]).astype(float)) / sum(d)),
}


@dataclass
class ValueProcessor:
    values: List[Any]
    timestamps: List[int]
    value_config_id: int
    method: str

    @property
    def durations(self):
        logging.warning("vp {}".format(self))
        if len(self.values) < 2:
            return np.array([])
        return np.array(self.timestamps[1:]) - np.array(self.timestamps[:-1])

    @property
    def value_count(self):
        return len(self.values) - 1

    @property
    def aggregate(self):
        f = BUILDIN_FUNCTIONS[self.method]
        return f(self.values, self.durations)

    def append_measurement(self, value, timestamp):
        self.values.append(value)
        self.timestamps.append(int(timestamp))

    def clear(self, new_timestamp: int = None):
        "keep only last record"
        if len(self.values) > 0:
            self.values = [self._last_not_none_value()]
            self.timestamps = [self.timestamps[-1]]
        if new_timestamp:
            if new_timestamp < self.timestamps[0]:
                raise ValueError(
                    "New timestamp {} is smaller than first timestamp {}".format(new_timestamp, self.timestamps[0])
                )
            self.timestamps = [new_timestamp]
        return self

    def _last_not_none_value(self):
        l = self.values
        l.reverse()
        return next(v for v in self.values if v is not None)

    def __str__(self):
        return f"ValueProcessor(values={self.values}, timestamps={self.timestamps})"


values = [2.7, 909, 792, 522]
timestamps = [1659085513316, 1659085516405, 1659085517436, 1659085518466]
v = ValueProcessor(values, timestamps, None, "mean")
assert v.aggregate == 341.9959805825243
assert v.value_count == 3
assert v.clear() == ValueProcessor([522], [1659085518466], None, "mean")
assert v.clear(1659085518467) == ValueProcessor([522], [1659085518467], None, "mean")
