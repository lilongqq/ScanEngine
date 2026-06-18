#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import logging.config
from datetime import datetime
from common.globals import variables
from crawling.files.iguocrawler import Iguo


def logging_config():
    logging.config.dictConfig(variables.logging)


def main():
    logging_config()
    content = """
-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQDBQ99XweJQ8oKXHUwdHyWlQx/0VSETQOAM91oVROL1QNkAnUbG
CIPh6zdcnYqIQbFt8OjK9KIrFnwma4uBIDGwzdcKyoI6OBMkKDbo5vpgBv9qm6+1
OgN810wtSN7ci0sVvr12Xy6wVaI0Iu0MXESastw8midLIFgWNIC/ZjIM5wIDAQAB
AoGAL3phD8oNsx0gf8cuv6i7MtI+s2OGcJhrckQB2V/G8cEwjjoU/OlynLmQLCx3
r+mTkRKx3cojXLp1XWrmJp6mFf/uATIfnmZBnpac3LXGDKP+pu8+8tQK/crCC03l
wnrpId9en9I9IaRDm6hdSwiUizDt4WGNwxIAbXY+6gMO/uECQQDiD021L8kUq9TA
EY5K4kW4c/+9R1IupNpEvISSHmHgOVBPle8PZGnWJSQjSiiLpvpqWPn/MaHrv7T7
S+U2OE7dAkEA2tylW4a3AfLG6YWoq/Nt6V1CUuXHK7bi2MfqGSDEb7wFyrvtD9rA
FXF88zf706zk81T8A/se7/nkEyqLaomUkwJBAIWuzyFq1NrokrPSrfcSwHBICOCC
INN8oacsZKmUVgUnX5rw66KKmxwMcsZ7wGZ1pHjnjU+gpkSn5fsF8tKRkfECQBzG
0DMllmB6NG819MSPIE+DxJmzvqlfxZntRzmAlnN+jGBorzXbdFAdeOld3g2p+PyJ
mw1G8n1pJPTkLiqW5mMCQDoznkwdq2SPfetXHdxNTHsdYdK9yzfrGJLl1mXOyAHr
mIcMkjQbjmNGPOsIKvdmZHjuWfaea5C6IIbna1w2+V0=
-----END RSA PRIVATE KEY-----
    """
    client = Iguo(
        url='https://igw.isgcc.net:18081',
        corpid='ww445f8033443a14aa',
        secret='v4rRQFoWwp1oucc2Mj4W-BwN0bVW2tO9jHgvOc3n-Pw'
    )
    start_time = int(
        datetime.fromisoformat('2024-01-09T13:49:29').timestamp()
    )
    end_time = int(
        datetime.fromisoformat('2025-01-09T13:49:29').timestamp()
    )
    logs = client.get_logs(
        feature_id=90000036,
        start_time=start_time,
        end_time=end_time
    )
    print(logs)

    # data = client.get_data(fileid='')
    # print(data)


if __name__ == "__main__":
    main()
