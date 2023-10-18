import timeit


class UtilMixin:
    @staticmethod
    def time_method(method):
        def timed(*args, **kwargs):
            ts = timeit.default_timer()
            result = method(*args, **kwargs)
            te = timeit.default_timer()
            print(f"{method.__name__} took {te - ts:.6f} seconds")
            return result

        return timed

    @classmethod
    def has_comparison_methods(cls, obj):
        try:
            # @TODO: better solution is needed here
            return obj < obj < obj == obj
        except TypeError:
            return False

    @staticmethod
    def print_metadata(schema, metadata, codec, serialized_size):
        print(f"Schema: {schema}")
        print(f"Metadata: {metadata}")
        print(f"Codec: {codec}")
        print(f"Serialized size: {serialized_size}")