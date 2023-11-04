FROM postgres:15.2
RUN apt-get update -y
RUN apt-get install postgresql-15-pglogical -y
RUN mkdir /var/lib/postgresql/data/pgdata -p
COPY postgresql.conf /var/lib/postgresql/data/pgdata/postgresql.conf
# RUN echo "shared_preload_libraries = 'pglogical'" | tee -a /var/lib/postgresql/data/pgdata/postgresql.conf
