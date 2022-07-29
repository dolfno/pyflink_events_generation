from pyflink.common.typeinfo import Types


class Measuremments:
    def flink_types(self):
        return Types.ROW_NAMED(
            ["id", "v", "q", "t"],
            [Types.STRING(), Types.STRING(), Types.BOOLEAN(), Types.STRING()],
        )
