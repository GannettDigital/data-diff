FROM python:3.10
RUN apt-get update && apt-get install -y \
    python3-dev libpq-dev wget unzip \
    python3-setuptools gcc bc
RUN pip install --no-cache-dir uv
COPY . /app
WORKDIR /app
# Install the project and extras into a uv-managed environment.
RUN uv sync --all-extras
ENTRYPOINT ["uv", "run", "python3", "-m", "data_diff"]
