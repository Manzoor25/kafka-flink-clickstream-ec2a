FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace
WORKDIR /workspace
COPY generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY schemas/ /workspace/schemas/
COPY generator/ /workspace/generator/
COPY scripts/ /workspace/scripts/
