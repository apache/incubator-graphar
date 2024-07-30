FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

RUN apt-get update && apt-get install -y ca-certificates lsb-release wget \
    && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y \
       cmake \
       build-essential \
       libarrow-dev \
       libarrow-dataset-dev \
       libarrow-acero-dev \
       libparquet-dev \
       libboost-graph-dev \
       doxygen \
       npm \
       default-jdk \
       git \
       vim \
       sudo \
       tzdata \
       maven \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && wget https://github.com/muttleyxd/clang-tools-static-binaries/releases/download/master-22538c65/clang-format-8_linux-amd64 -O /usr/bin/clang-format \
    && chmod +x /usr/bin/clang-format \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN git clone --branch v1.8.3 https://github.com/google/benchmark.git /tmp/benchmark --depth 1 \
    && cd /tmp/benchmark \
    && cmake -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_TESTING=OFF -DBENCHMARK_ENABLE_GTEST_TESTS=OFF . \
    && make -j`nproc` \
    && make install \
    && rm -rf /tmp/benchmark

RUN git clone --branch v3.6.0 https://github.com/catchorg/Catch2.git /tmp/catch2 --depth 1 \
    && cd /tmp/catch2 \
    && cmake -Bbuild -H. -DBUILD_TESTING=OFF \
    && cmake --build build/ --target install \
    && rm -rf /tmp/catch2

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN useradd -m graphar -u 1001 \
    && echo 'graphar ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER graphar
WORKDIR /home/graphar
