FROM python:3.11.3-slim-bullseye
WORKDIR /app

# For installing deps
COPY ./requirements.txt .

# Update and install deps
RUN apt-get update && \
    pip3 install -r requirements.txt

ENV PYTHONUNBUFFERED=True \
    WORKERS=2 \
    THREADS=4 \
    PORT=8501 \
    SPACY_MODEL=ru_core_news_md \
    API_PORT=80 \
    API_HOST=localhost

RUN python -m spacy download $SPACY_MODEL
RUN python -m spacy download en_core_web_sm

EXPOSE $PORT

# Copy relevant source code
COPY . .

# Add group AND user 'fastapi', change fastapi_app workdir ownership
RUN groupadd -r xuser && \
    useradd -r -g xuser xuser --home-dir /app && \
    chown -R xuser:xuser /app/

# Change to least privilege user (for security reasons)
USER xuser

CMD streamlit run app.py --server.port $PORT