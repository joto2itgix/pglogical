#!/usr/bin/python3.10
# docker run -it --rm --network docker_pglogical postgres psql -h 10.200.200.10 -U postgres

# READ CONFIG
import yaml
with open("config.yaml", "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from classes.pglogical import PGLogical
from classes.postgresql import Postgresql
from classes.logs import Logs

pglogical = PGLogical()
postgresql = Postgresql()
logs = Logs()

# CREATE PUBLICATIONS
for db in config["db_list"]:
    conn_psql1 = psycopg2.connect(
        database=db["name"],
        host=config["src_db"]["host"],
        user=config["src_db"]["user"],
        password=config["src_db"]["pass"],
        port=config["src_db"]["port"]
    )
    conn_psql1.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn_psql2 = psycopg2.connect(
        host=config["dest_db"]["host"],
        user=config["dest_db"]["user"],
        password=config["dest_db"]["pass"],
        port=config["dest_db"]["port"]
    )
    conn_psql2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # SLAVE CREATE DATABASE
    postgresql.executeQuery( conn_psql2, "CREATE DATABASE "+db["name"]+";" )
    conn_psql2.close()

    # SLAVE RECONNECT TO THE NEW DB
    conn_psql2 = psycopg2.connect(
        database=db["name"],
        host=config["dest_db"]["host"],
        user=config["dest_db"]["user"],
        password=config["dest_db"]["pass"],
        port=config["dest_db"]["port"]
    )
    conn_psql2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # MASTER CREATE EXTENTION
    postgresql.executeQuery( conn_psql1, "CREATE EXTENSION pglogical;" )

    # MASTER CREATE NODE
    query = "SELECT pglogical.create_node('%s_m', 'host=%s port=%s dbname=%s');" % (
        db["name"],
        config["src_db"]["host"],
        config["src_db"]["port"],
        db["name"]
    )
    postgresql.executeQuery( conn_psql1, query )

    # MASTER CREATE REPLICATION SET
    query = "SELECT pglogical.create_replication_set('%s_set', %s, %s, %s, %s);" % (
        db["name"],
        db["insert"],
        db["update"],
        db["delete"],
        db["truncate"]
    )
    postgresql.executeQuery( conn_psql1, query )

    # SYNC TABLES
    tables = postgresql.executeQuery( conn_psql1, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';" )
    # FILTER IGNORED TABLES
    for table in reversed(tables):
        if table[0] in db["ignore_tables"]:
            tables.remove(table)
    # DUMP AND IMPORT TABLES SCHEMA
    for table in tables:
        # CHECK IF EXISTS ON SLAVE
        query = "select exists(select * from information_schema.tables where table_name='%s');" % table[0]
        exitCheck = postgresql.executeQuery( conn_psql2, query )
        if (not exitCheck):
            command = "docker run -it --rm -v $(pwd)/docker/data/tmp:/tmp -e PGPASSWORD=%s --network docker_pglogical postgres \
                pg_dump -h %s -U %s --table=%s -s --file=/tmp/%s.sql %s" % (
                config["src_db"]["pass"],
                config["src_db"]["host"],
                config["src_db"]["user"],
                table[0],
                table[0],
                db["name"]
            )
            postgresql.executeBash (command)

            command = "docker run -it --rm -v $(pwd)/docker/data/tmp:/tmp -e PGPASSWORD=%s --network docker_pglogical postgres \
                psql -h %s -U %s -f /tmp/%s.sql %s" % (
                config["dest_db"]["pass"],
                config["dest_db"]["host"],
                config["dest_db"]["user"],
                table[0],
                db["name"]
            )
            postgresql.executeBash (command)
        
        # MASTER ADD TABLE TO SET
        query = "SELECT pglogical.replication_set_add_table('%s_set', '%s', true);" % (
            db["name"],
            table[0]
        )
        postgresql.executeQuery( conn_psql1, query )

    # SLAVE CREATE EXTENTION
    postgresql.executeQuery( conn_psql2, "CREATE EXTENSION pglogical;" )

    # SLAVE CREATE NODE
    query = "SELECT pglogical.create_node('%s_wh', 'host=%s port=%s dbname=%s');" % (
        db["name"],
        config["dest_db"]["host"],
        config["dest_db"]["port"],
        db["name"],
    )
    postgresql.executeQuery( conn_psql2, query )

    # SLAVE CREATE SUBSCRIPTION
    query = "SELECT pglogical.create_subscription('%s_subscription', 'host=%s port=%s dbname=%s', array['%s_set'], false, false);" % (
        db["name"],
        config["src_db"]["host"],
        config["src_db"]["port"],
        db["name"],
        db["name"]
    )
    postgresql.executeQuery( conn_psql2, query )

    # SLAVE RESYNC SUBSCRIPTION
    query = "SELECT pglogical.alter_subscription_synchronize('%s_subscription', false);" % (
        db["name"]
    )
    postgresql.executeQuery( conn_psql2, query )

    # CLOSE ACTIVE CONNECTIONS
    conn_psql1.close()
    conn_psql2.close()
