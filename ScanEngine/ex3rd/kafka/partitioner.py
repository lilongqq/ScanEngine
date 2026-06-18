#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
from kafka.partitioner import murmur2


class CustomPartitioner(object):

    def __init__(self, partitions):
        self.partitions = partitions

    def __call__(self, key, all_partitions, available):
        if self.partitions:
            if key is None:
                available = self.intersection(self.partitions, available)
                if available:
                    return random.choice(available)
                return random.choice(self.partitions)

            idx = murmur2(key)
            idx &= 0x7fffffff
            idx %= len(self.partitions)
            return self.partitions[idx]
        else:
            if key is None:
                if available:
                    return random.choice(available)
                return random.choice(all_partitions)

            idx = murmur2(key)
            idx &= 0x7fffffff
            idx %= len(all_partitions)
            return all_partitions[idx]

    @staticmethod
    def intersection(partitions, available):
        set1 = set(partitions)
        set2 = set(available)
        return list(set1 & set2)
