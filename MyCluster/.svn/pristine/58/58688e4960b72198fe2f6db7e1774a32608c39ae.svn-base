#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime
import json


def default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return json.dumps(obj, ensure_ascii=False, indent=4)
