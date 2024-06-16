FROM python:3.11.3-slim-bullseye
WORKDIR /code

# For installing deps
COPY requirements.txt .

# Update and install deps
RUN apt-get update && apt-get install -y gcc g++ make libgomp1 && \
    pip3 install --upgrade pip  && \
    pip3 install -r requirements.txt

# Install utils for debugging: ping, curl, nano
RUN apt-get install -y iputils-ping curl nano

ENV PYTHONUNBUFFERED=True \
    WORKERS=2 \
    THREADS=4 \
    SPACY_MODEL=ru_core_news_md \
    MODELS_PATH=/code/backend/checkpoints/ \
    BEST_MODEL=model_016

RUN python -m spacy download $SPACY_MODEL
RUN python -m spacy download en_core_web_sm

######################################### Oracle Instant Client #########################################
# Install Oracle dependencies
# see https://help.ubuntu.com/community/Oracle%20Instant%20Client
RUN apt-get update && apt-get install -y --no-install-recommends alien libaio1 wget && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm
ENV LD_LIBRARY_PATH="/usr/lib/oracle/18.5/client64/lib:${LD_LIBRARY_PATH}"

# Install cx_Oracle
RUN pip3 install cx_Oracle
########################################################################################################

# Copy relevant source code
COPY . ./backend

# Add group AND user 'fastapi', change code workdir ownership
RUN groupadd -r fastapi && \
    useradd -r -g fastapi fastapi --home-dir /code && \
    chown -R fastapi:fastapi /code/

# Change ownership and permissions for Oracle Instant Client
#RUN mkdir -p /opt/oracle/instantclient_21_1 && \
#    chown -R fastapi:fastapi /opt/oracle/instantclient_21_1 && \
#    chmod -R 755 /opt/oracle/instantclient_21_1

# Change to least privilege user (for security reasons)
USER fastapi

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--port", "8000", "--host", "0.0.0.0", "--reload"]