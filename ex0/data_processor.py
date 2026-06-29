#!/usr/bin/env python3

import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:

        if self._data:
            rank = self._rank
            self._rank += 1
            return (rank, self._data.pop(0))
        else:
            return (-1, "No data available")


class NumericProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:

        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(i, (int, float)) for i in data)

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:

        if self.validate(data):
            if isinstance(data, (int, float)):
                self._data.append(str(data))
            else:
                for i in data:
                    self._data.append(str(i))
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:

        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(i, str) for i in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, str):
                self._data.append(data)
            else:
                for i in data:
                    self._data.append(i)
        else:
            raise ValueError("Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:

        if isinstance(data, dict):
            return all(isinstance(k, str) and isinstance(v, str)
                       for k, v in data.items())

        if isinstance(data, list):
            return all(all(isinstance(k, str) and isinstance(v, str)
                       for k, v in i.items())
                       for i in data)

        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:

        if self.validate(data):

            if isinstance(data, dict):
                self._data.append(
                    data["log_level"] + ": " + data["log_message"]
                    )
            else:
                for i in data:
                    self._data.append(
                        i["log_level"] + ": " + i["log_message"]
                    )
        else:
            raise ValueError("Improper dict data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("Testing Numeric Processor...")
    num = NumericProcessor()
    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")
    try:
        print("Test invalid ingestion of string "
              "'foo' without prior validation:")
        num.ingest("foo")  # type: ignore
    except ValueError as error:
        print(f"Got exception: {error}")
    num_list: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_list}")
    print("Extracting 3 values...")
    num.ingest(num_list)
    num_tuple = num.output()
    print(f"Numeric value {num_tuple[0]}: {num_tuple[1]}")
    num_tuple = num.output()
    print(f"Numeric value {num_tuple[0]}: {num_tuple[1]}")
    num_tuple = num.output()
    print(f"Numeric value {num_tuple[0]}: {num_tuple[1]}")

    print("Testing Text Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")
    text_list = ["Hello", "Nexus", "World"]
    print(f"Processing data: {text_list}")
    text.ingest(text_list)
    print("Extracting 1 value...")
    text_tuple = text.output()
    print(f"Text value {text_tuple[0]}: {text_tuple[1]}")

    print("Testing Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")
    log_list = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
                ]
    print(f"Processing data: {log_list}")
    log.ingest(log_list)
    print("Extracting 2 values...")
    log_tuple = log.output()
    print(f"Log entry {log_tuple[0]}: {log_tuple[1]}")
    log_tuple = log.output()
    print(f"Log entry {log_tuple[0]}: {log_tuple[1]}")
