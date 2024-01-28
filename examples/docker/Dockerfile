FROM bitnami/minideb:bookworm

RUN install_packages \
    ca-certificates \
    wget \
    curl \
    pkg-config \
    gcc \
    man-db \
    g++ \
    make \
    autoconf \
    unzip \
    bzip2 \
    zlib1g-dev \
    dpkg-dev \
    build-essential \
	libncurses5-dev \
    libcurl4-openssl-dev \
    libbz2-dev \
    liblzma-dev


# install gcloud
ENV GCLOUD_SDK_VERISON="461.0.0"
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
    && tar xzf google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
    && /google-cloud-sdk/install.sh -q \
    && /google-cloud-sdk/bin/gcloud components update --quiet

ENV PATH=/google-cloud-sdk/bin:$PATH


# install htslib  --  install in downstream images with:
#     COPY --from=weisburd/base-bam-tools:latest  /usr/local/bin/tabix /usr/local/bin/tabix
#     COPY --from=weisburd/base-bam-tools:latest  /usr/local/bin/htsfile /usr/local/bin/htsfile
#     COPY --from=weisburd/base-bam-tools:latest  /usr/local/bin/bgzip /usr/local/bin/bgzip
#     COPY --from=weisburd/base-bam-tools:latest  /usr/local/bin/samtools /usr/local/bin/samtools
ENV SAMTOOLS_VERSION="1.19.1"
RUN wget https://github.com/samtools/htslib/releases/download/${SAMTOOLS_VERSION}/htslib-${SAMTOOLS_VERSION}.tar.bz2 \
	&& tar xjf htslib-${SAMTOOLS_VERSION}.tar.bz2 \
	&& rm htslib-${SAMTOOLS_VERSION}.tar.bz2 \
	&& cd htslib-${SAMTOOLS_VERSION} \
	&& ./configure \
	&& make \
	&& make install \
	&& make clean

# install samtools  --  install in downstream images with:
#     COPY --from=weisburd/base-bam-tools:latest /usr/local/bin/samtools /usr/local/bin/samtools
RUN wget https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2 \
	&& tar xjf samtools-${SAMTOOLS_VERSION}.tar.bz2 \
	&& rm samtools-${SAMTOOLS_VERSION}.tar.bz2 \
	&& cd samtools-${SAMTOOLS_VERSION} \
	&& ./configure \
	&& make \
	&& make install \
	&& make clean

# install bwa  --  install in downstream images with:
#     COPY --from=weisburd/base-bam-tools:latest  /usr/local/bin/bwa /usr/local/bin/bwa
RUN wget https://github.com/lh3/bwa/archive/master.zip -O bwa.zip \
	&& unzip bwa.zip \
	&& cd bwa-master \
	&& make \
	&& cp bwa /usr/local/bin/ \
	&& cd .. \
	&& rm -rf bwa*
