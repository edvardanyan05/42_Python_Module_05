#!/usr/bin/env python3

import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0
        self._processed: int = 0

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
                self._processed += 1
            else:
                for i in data:
                    self._data.append(str(i))
                    self._processed += 1
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
                self._processed += 1
            else:
                for i in data:
                    self._data.append(i)
                    self._processed += 1
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
                self._processed += 1
            else:
                for i in data:
                    self._data.append(
                        i["log_level"] + ": " + i["log_message"]
                    )
                    self._processed += 1
        else:
            raise ValueError("Improper dict data")


class DataStream:

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            for proc in self._processors:
                if proc.validate(item):
                    proc.ingest(item)
                    break
            else:
                print(f"DataStream error - "
                      f"Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(f"{proc.__class__.__name__}: total {proc._processed} "
                  f"items processed, remaining {len(proc._data)} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    ex_stream = DataStream()
    ex_stream.print_processors_stats()

    print("Registering Numeric Processor")
    num = NumericProcessor()
    ex_stream.register_processor(num)

    process_list = ['Hello world',
                    [3.14, -1, 2.71],
                    [{'log_level': 'WARNING',
                      'log_message': 'Telnet access! Use ssh instead'},
                     {'log_level': 'INFO',
                      'log_message': 'User wil isconnected'}], 42,
                    ['Hi', 'five']
                    ]
    print(f"Send first batch of data on stream: {process_list}")
    ex_stream.process_stream(process_list)
    ex_stream.print_processors_stats()

    print("Registering other data processors")
    txt = TextProcessor()
    log = LogProcessor()
    ex_stream.register_processor(txt)
    ex_stream.register_processor(log)
    print("Send the same batch again")
    ex_stream.process_stream(process_list)
    ex_stream.print_processors_stats()

    print("Consume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")

    num.output()
    num.output()
    num.output()
    txt.output()
    txt.output()
    log.output()
    ex_stream.print_processors_stats()
