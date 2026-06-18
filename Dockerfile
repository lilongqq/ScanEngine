FROM python:3.11-slim-bookworm
ENV LANG=en_US.UTF-8
ADD config /opt/config
ADD certs /opt/certs
ADD log /opt/log
ADD strategy /opt/
ADD pypackages /opt/pypackages
ADD debian /opt/debian
ADD debpackages /opt/debpackages/
ADD debian-archive-bookworm-shipinginfo.asc /etc/apt/trusted.gpg.d/debian-archive-bookworm-shipinginfo.asc
ADD sources.list /etc/apt/sources.list
ADD pip.conf /etc/pip.conf
ADD ScanEngine /opt/ScanEngine
RUN rm -rf /etc/apt/sources.list.d/* && \
    apt update && \
    apt install -y \
    iputils-ping \
    curl \
    unrar \
    gconf-service \
    libasound2 \
    libatk1.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgcc1 \
    libgconf-2-4 \
    libgdk-pixbuf2.0-0 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    libnss3 && \
    apt clean && \
    pip3 install -r /opt/pypackages/requirements.txt --no-index -f /opt/pypackages/`uname -m`/ && \
    pip3 install -r /opt/pypackages/requirements-net.txt && \
    pip3 install pymongo==3.13.0 --target=/opt/legacy-packages/pymongo==3.13.0 --no-index -f /opt/pypackages/`uname -m`/ && \
    pip cache purge
RUN dpkg -i /opt/debpackages/`uname -m`/*.deb && rm -fr /opt/debpackages /opt/pypackages
ADD chromium_downloader.py /usr/local/lib/python3.11/site-packages/pyppeteer/chromium_downloader.py
ADD requests_html.py /usr/local/lib/python3.11/site-packages/requests_html.py
WORKDIR /opt
CMD python3 ScanEngine