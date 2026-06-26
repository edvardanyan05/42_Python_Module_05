#!/usr/bin/env python3

import abc

class DataProcessor(abc.ABC):
    @abc.abstractcmethod
    def validate(self, data):
        pass

    @abc.abstractcmethod
    def ingest(self, data):
        pass