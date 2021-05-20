FROM python:3.8-slim-buster

WORKDIR /code
COPY . .
RUN pip3 install 'psycopg2-binary==2.8.6' 'pyyaml==5.3.1' 'skytools==3.6.1' 'pgq==3.5'
RUN pip3 install .

ENV DEBIAN_FRONTEND="noninteractive"
ENV PG=12
RUN apt="apt-get -qq -y --no-install-recommends"; \
	${apt} update; \
	${apt} upgrade; \
	${apt} install wget gnupg2 lsb-release git make gcc;

RUN set -ex; \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -; \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main ${PG}" |  \
    tee /etc/apt/sources.list.d/pgdg.list; \
    apt-get -q update;

# disable new cluster creation
RUN set -ex; \
    mkdir -p /etc/postgresql-common/createcluster.d; \
    echo "create_main_cluster = false" | tee /etc/postgresql-common/createcluster.d/no-main.conf; \
    apt-get -qyu install postgresql-${PG} postgresql-server-dev-${PG} pgqd;

ENV PATH="/usr/lib/postgresql/${PG}/bin:${PATH}"
ENV PGHOST="/tmp"

RUN set -ex; \
    git clone -q https://github.com/pgq/pgq; make -C pgq; \
    bash -c "PATH='${PATH}' make install -C pgq";

RUN set -ex; \
    git clone -q https://github.com/pgq/pgq-node; make -C pgq-node; \
    bash -c "PATH='${PATH}' make install -C pgq-node";

RUN set -ex; \
    git clone -q https://github.com/pgq/londiste-sql; make -C londiste-sql; \
    bash -c "PATH='${PATH}' make install -C londiste-sql";

RUN set -ex; \
    chown -R postgres:postgres "/var/lib/postgresql/${PGVER}"; \
    chmod -R u+rwX,g-rwx,o-rwx "/var/lib/postgresql/${PGVER}"; \
    chown -R postgres:postgres "/usr/share/postgresql/${PGVER}"; \
    chmod -R u+rwX,g-rwx,o-rwx "/usr/share/postgresql/${PGVER}"; \
    chown -R postgres:postgres "."; \
    chmod -R u+rwX,g-rwx,o-rwx ".";

USER postgres

RUN set -ex; \
    rm -rf data log; \
    mkdir -p log; \
    LANG=C initdb data; \
    sed -ri -e "s,^[# ]*(unix_socket_directories).*,\\1='/tmp'," data/postgresql.conf;

ENTRYPOINT ["tests/docker_run.sh"]

