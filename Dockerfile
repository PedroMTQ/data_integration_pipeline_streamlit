FROM python:3.11.3-bullseye AS build
# installs uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# install java for Spark, could be singled out into a separate base image
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN mkdir app
COPY ./uv.lock app/uv.lock
COPY ./pyproject.toml app/pyproject.toml
COPY ./README.md app/README.md

# Extract version and add to a __version__ file which will be read by the service later
RUN echo $(grep -m 1 'version' app/pyproject.toml | sed -E 's/version = "(.*)"/\1/') > /app/__version__

# copying source code
COPY ./src/ app/src/
WORKDIR "/app"

# installs package
RUN uv sync --all-extras
ENV PATH="/app/.venv/bin:$PATH"


# Final
FROM build
RUN apt autoremove -y && apt clean -y
