# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging.config
import traceback
from kafka.errors import KafkaError
from mq import Consumer
from common.globals import Variables
from strategy import StrategyFactory


class ScanEngine(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.variables = Variables()
        self.strategy_factory = StrategyFactory()
        self.operator_consumer = Consumer(
            topic=self.variables.kafka['definition'],
            partition=self.variables.kafka['partition'],
            ** self.variables.kafka['consumer']
        )
        self.attribute_consumer = Consumer(
            topic=self.variables.kafka['attributes'],
            partition=self.variables.kafka['partition'],
            ** self.variables.kafka['consumer']
        )

    def resume(self):
        if self.attribute_consumer.get_lag():
            self.strategy_factory.slave_resume()

    def run(self):
        self.resume()
        while True:
            try:
                message = next(self.operator_consumer)
                self.logger.info(
                    "[Topic=%s][Partition=%d][Offset=%d]",
                    message.topic,
                    message.partition,
                    message.offset
                )
                definition = message.value
                self.logger.info(definition)
                op = definition.get('op', 'create')
                identity = definition.get('identity')
                self.logger.info(definition)
                if op == 'create':
                    self.strategy_factory.create(definition)
                else:
                    func = getattr(self.strategy_factory, op)
                    func(identity)
            except KafkaError:
                self.logger.error(traceback.format_exc())
            except StopIteration:
                break
            except Exception:
                self.logger.info(traceback.format_exc())
            finally:
                self.operator_consumer.commit()
