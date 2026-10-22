FROM --platform=linux/amd64 rocker/r-ver:4.4.3

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

# Restore the exact package set recorded in renv.lock (CRAN versions from a
# dated P3M snapshot, GitHub packages at pinned commit SHAs).
# Copying only the lockfile first keeps this layer cached until it changes.

RUN R -e "install.packages('renv', repos = 'https://p3m.dev/cran/__linux__/noble/2025-04-10')"

COPY renv.lock /tmp/renv.lock

RUN R -e "options(timeout = 1200); renv::restore(lockfile = '/tmp/renv.lock', library = '/usr/local/lib/R/site-library', prompt = FALSE)"

# Install HMISserve from the build context without touching the pinned
# dependencies (the PR's code, or master's code on master builds)

COPY . /tmp/HMISserve

RUN R CMD INSTALL --no-deps /tmp/HMISserve

# Create app directory and set working directory

WORKDIR /app

# Copy the processing script

COPY aws/process_hmis_serve_data.R ./

# Make script executable and add shebang if needed

RUN chmod +x process_hmis_serve_data.R

# Run the script when container starts

CMD ["Rscript", "process_hmis_serve_data.R"]