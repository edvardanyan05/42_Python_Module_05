#!/usr/bin/env python3

import abc
import typing


class DataStream():

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []
        
    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            if any(proc.validate(item) for proc in self._processors):
                for proc in self._processors:
                    if proc.validate(item):
                        proc.ingest(item)
                        break
            else:
                print(f"DataStream error - Can't process element in stream: {item}")


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
