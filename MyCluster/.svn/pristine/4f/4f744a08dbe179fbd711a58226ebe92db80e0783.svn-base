FROM python:3.9.18-slim-bullseye
ENV LANG=en_US.UTF-8
ADD config /opt/config
ADD certs /opt/certs
ADD log /opt/log
ADD strategy /opt/
ADD pypackages /opt/pypackages
ADD ScanEngine /opt/ScanEngine
RUN pip3 install -r /opt/pypackages/requirements.txt --no-index -f /opt/pypackages/`uname -m`/

RUN pip3 install pymongo==4.3.3 --target=/opt/ScanEngine/libs/mongodb/pymongo433 --no-index -f /opt/pypackages/`uname -m`/ && pip3 install pymongo==3.5.1 --target=/opt/ScanEngine/libs/mongodb/pymongo351 --no-index -f /opt/pypackages/`uname -m`/ && pip3 install pymongo==3.13.0 --target=/opt/ScanEngine/libs/mongodb/pymongo313 --no-index -f /opt/pypackages/`uname -m`/

WORKDIR /opt
CMD python3 ScanEngine

