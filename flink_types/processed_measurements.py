from pyflink.common.typeinfo import Types
from pyflink.common import Row


class ProcessedMeasurements:
    def flink_types(self):
        return Types.ROW_NAMED(
            [
                "category",
                "state_config_id",
                "result_value",
                "ts_start",
                "ts_end",
                "value_config_id",
                "events_id",
                "value_count",
            ],
            [
                Types.STRING(),
                Types.INT(),
                Types.STRING(),
                Types.LONG(),
                Types.LONG(),
                Types.INT(),
                Types.STRING(),
                Types.INT(),
            ],
        )

    def create_row(
        self,
        category,
        state_config_id,
        result_value,
        ts_start,
        ts_end=None,
        value_config_id=None,
        event_id=None,
        value_count=None,
    ):
        for i in [category, state_config_id, result_value, ts_start]:
            if i is None:
                raise ValueError(f"{i} is None")

        if ts_end:
            ts_end = int(ts_end)

        if value_config_id:
            value_config_id = int(value_config_id)

        if event_id:
            event_id = str(event_id)

        if value_count:
            value_count = int(value_count)

        return Row(
            str(category),
            int(state_config_id),
            str(result_value),
            int(ts_start),
            ts_end,
            value_config_id,
            event_id,
            value_count,
        )
