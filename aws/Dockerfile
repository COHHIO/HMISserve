FROM --platform=linux/amd64 rocker/tidyverse:4.4

ARG GITHUB_PAT
ENV GITHUB_PAT=${GITHUB_PAT}

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    git \
    libcurl4-openssl-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libfribidi-dev \
    libgit2-dev \
    libharfbuzz-dev \
    libicu-dev \
    libjpeg-dev \
    libpng-dev \
    libssh2-1-dev \
    libssl-dev \
    libtiff5-dev \
    libxml2-dev \
    make \
    pandoc \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install required R packages
RUN R -e "install.packages(c('pak', 'logger', 'aws.s3', 'dplyr', 'stringr'), repos='https://cloud.r-project.org/')"

RUN R -e "pak::cache_clean()"

# Install GitHub packages individually with proper PAT setup and timeout
RUN R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT')); options(pak.no_prompt = TRUE, timeout = 1200); pak::pak('yogat3ch/UU')"
RUN R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT')); options(pak.no_prompt = TRUE, timeout = 1200); pak::pak('COHHIO/HMIS')"
RUN R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT')); options(pak.no_prompt = TRUE, timeout = 1200); pak::pak('COHHIO/HMISdata')"
RUN R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT')); options(pak.no_prompt = TRUE, timeout = 1200); pak::pak('COHHIO/HMISprep')"
RUN R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT')); options(pak.no_prompt = TRUE, timeout = 1200); pak::pak('COHHIO/clarity.looker')"
RUN R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT')); options(pak.no_prompt = TRUE, timeout = 1200); pak::pak('COHHIO/HMISserve')"

# Create app directory and set working directory
WORKDIR /app

# Create directory for service account file
RUN mkdir -p inst/vault

# Copy the processing script
COPY process_hmis_serve_data.R ./

# Make script executable and add shebang if needed
RUN chmod +x process_hmis_serve_data.R

# Run the script when container starts
CMD ["Rscript", "process_hmis_serve_data.R"]