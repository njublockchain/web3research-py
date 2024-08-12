import base58
from binascii import unhexlify
from typing import Generator, Optional
from web3research.common.types import ChainStyle


def convert_bytes_to_hex(raw: bytes, style: ChainStyle):
    if style == ChainStyle.ETH:
        return "0x" + raw.hex()
    elif style == ChainStyle.TRON:
        if len(raw) == 20:
            return base58.b58encode_check(unhexlify("41" + raw.hex()))
        return raw.hex()


def convert_bytes_to_hex_generator(
    generator: Optional[Generator[dict, None, None]], style: ChainStyle
):
    if generator is None:
        return generator

    for item in generator:
        for key, value in item.items():
            if isinstance(value, bytes):
                item[key] = convert_bytes_to_hex(value, style)
            elif isinstance(value, dict):
                for k, v in value.items():
                    if isinstance(v, bytes):
                        value[k] = convert_bytes_to_hex(v, style)
                item[key] = value
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    if isinstance(v, bytes):
                        value[i] = convert_bytes_to_hex(v, style)
                item[key] = value
            elif isinstance(value, tuple):
                value = list(value)
                for i, v in enumerate(value):
                    if isinstance(v, bytes):
                        value[i] = convert_bytes_to_hex(v, style)
                item[key] = tuple(value)
            elif isinstance(value, set):
                value = list(value)
                for i, v in enumerate(value):
                    if isinstance(v, bytes):
                        value[i] = convert_bytes_to_hex(v, style)
                item[key] = set(value)
            elif isinstance(value, frozenset):
                value = list(value)
                for i, v in enumerate(value):
                    if isinstance(v, bytes):
                        value[i] = convert_bytes_to_hex(v, style)
                item[key] = frozenset(value)

        yield item


def group_events_generator(generator: Optional[Generator[dict, None, None]]):
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
