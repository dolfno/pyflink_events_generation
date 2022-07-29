from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import WatermarkStrategy, Duration

"""
>>> Example
ds.assign_timestamps_and_watermarks(ws)
"""


class AssignMeasurementTimestamp(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value.t)


ws = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2)).with_timestamp_assigner(
    AssignMeasurementTimestamp()
)
