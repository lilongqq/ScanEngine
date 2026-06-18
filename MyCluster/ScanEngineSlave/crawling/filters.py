#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from functools import partial
from exceptions import FilterError


class FilterChain(object):

    def __init__(self, kwargs):
        self.chain = list()
        self.generate(kwargs)

    @staticmethod
    def lt(item, value, node):
        if item in node:
            try:
                node_val = type(value)(node.get(item))
            except (TypeError, ValueError):
                raise FilterError(item)
            if node_val < value:
                return True
            else:
                raise FilterError(item)
        else:
            return True

    @staticmethod
    def le(item, value, node):
        if item in node:
            try:
                node_val = type(value)(node.get(item))
            except (TypeError, ValueError):
                raise FilterError(item)
            if node_val <= value:
                return True
            else:
                raise FilterError(item)
        else:
            return True

    @staticmethod
    def eq(item, value, node):
        if item in node:
            try:
                node_val = type(value)(node.get(item))
            except (TypeError, ValueError):
                raise FilterError(item)
            if node_val == value:
                return True
            else:
                raise FilterError(item)
        else:
            return True

    @staticmethod
    def ne(item, value, node):
        if item in node:
            try:
                node_val = type(value)(node.get(item))
            except (TypeError, ValueError):
                raise FilterError(item)
            if node_val != value:
                return True
            else:
                raise FilterError(item)
        else:
            return True

    @staticmethod
    def gt(item, value, node):
        if item in node:
            try:
                node_val = type(value)(node.get(item))
            except (TypeError, ValueError):
                raise FilterError(item)
            if node_val > value:
                return True
            else:
                raise FilterError(item)
        else:
            return True

    @staticmethod
    def ge(item, value, node):
        if item in node:
            try:
                node_val = type(value)(node.get(item))
            except (TypeError, ValueError):
                raise FilterError(item)
            if node_val >= value:
                return True
            else:
                raise FilterError(item)
        else:
            return True

    @staticmethod
    def include(item, suffixes, node):
        value = node.get(item).lower()
        for suffix in suffixes:
            if value.endswith(suffix):
                return True
        else:
            raise FilterError(item)

    @staticmethod
    def exclude(item, suffixes, node):
        value = node.get(item).lower()
        for suffix in suffixes:
            if value.endswith(suffix):
                raise FilterError(item)
        else:
            return True

    def generate(self, kwargs):
        for item, fun in kwargs.items():
            for t, value in fun.items():
                self.chain.append(
                    partial(getattr(self, t), item, value)
                )

    def add(self, fun):
        self.chain.append(fun)

    def __call__(self, node):
        for fun in self.chain:
            if fun(node):
                continue
            else:
                return False
        else:
            return True
