#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod


class Iterator(ABC):

    @abstractmethod
    def __init__(self):
        raise NotImplemented

    @abstractmethod
    def __iter__(self):
        raise NotImplemented

    @abstractmethod
    def __next__(self):
        """
        return:
            node: the file meta dict
            stream: you can get the file data by call stream()
        """
        raise NotImplemented


class Client(ABC):

    @abstractmethod
    def __init__(self):
        raise NotImplemented

    @abstractmethod
    def __bool__(self):
        raise NotImplemented

    @abstractmethod
    def __del__(self):
        raise NotImplemented
