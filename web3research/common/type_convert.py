from typing import Any, Generator


def convert_bytes_to_hex_generator(generator: Generator[dict, None, None]):
    for item in generator:

        for key, value in item.items():
            if isinstance(value, bytes):
                item[key] = value.hex()

        yield item


def group_events_generator(generator: Generator[dict, None, None]):
    if generator is None:
        return generator

    for event in generator:
        event["topics"] = []
        # restruct the topics
        if event["topic0"] is not None:
            event["topics"].append(event["topic0"])
        if event["topic1"] is not None:
            event["topics"].append(event["topic1"])
        if event["topic2"] is not None:
            event["topics"].append(event["topic2"])
        if event["topic3"] is not None:
            event["topics"].append(event["topic3"])

        del event["topic0"], event["topic1"], event["topic2"], event["topic3"]

        yield event