#!/usr/bin/python3.10

import time
from os import path
import yaml
from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import yaml
with open("config.yaml", "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

from classes.pglogical import PGLogical
from classes.postgresql import Postgresql
from classes.logs import Logs

pglogical = PGLogical()
postgresql = Postgresql()
logs = Logs()


class PGLogicalCollector(object):
    def __init__(self):
        pass
    def collect(self):
        #SLOTS
        gaugeName = "sum_pg_ls_waldir"
        gaugeInfo = "Size of wal folder files"
        gaugeLSWalDir = GaugeMetricFamily(gaugeName, gaugeInfo, labels=["host", "database"])
        gaugeName = "select_min_wal_size"
        gaugeInfo = "Min wal size"
        gaugeMinWalSize = GaugeMetricFamily(gaugeName, gaugeInfo, labels=["host", "database"])
        gaugeName = "select_max_wal_size"
        gaugeInfo = "Max wal size"
        gaugeMaxWalSize = GaugeMetricFamily(gaugeName, gaugeInfo, labels=["host", "database"])

        gaugeName = "select_pg_replication_slots"
        gaugeInfo = "Show Active Replication Slots"
        gaugeActiveReplicationSlots = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "slot_name", "database", "restart_lsn", "confirmed_flush_lsn", "wal_status", "safe_wal_size"
        ])
        gaugeName = "select_pg_replication_slots_wal"
        gaugeInfo = "Show Wal Size of Replication Slots"
        gaugeSlotsWal = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "slot_name", "database", "restart_lsn", "confirmed_flush_lsn", "wal_status"
        ])
        gaugeName = "sql_show_max_replication_slots"
        gaugeInfo = "Show Total Replication Slots"
        gaugeTotalReplicationSlots = GaugeMetricFamily(gaugeName, gaugeInfo, labels=["host", "database"])

        gaugeName = "select_pg_stat_replication"
        gaugeInfo = "Show all replications"
        gaugeReplication = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "application_name", "client_addr", "state", "sent_lsn", "write_lsn", "flush_lsn", "replay_lsn"
        ])
        
        gaugeName = "select_pglogical_node"
        gaugeInfo = "Select replication nodes"
        gaugeListNodes = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "node_id", "node_name"
        ])

        gaugeName = "select_pglogical_replication_set"
        gaugeInfo = "Select replication sets on master"
        gaugeListReplicationSets = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "set_id", "set_nodeid", "set_name", "replicate_insert", "replicate_update", "replicate_delete", "replicate_truncate"
        ])

        gaugeName = "select_pglogical_subscriptions"
        gaugeInfo = "Select replication subscriptions from slave"
        gaugeListReplicationSubscriptions = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "subscription_name", "status", "provider_node", "provider_dsn", "slot_name", "replication_sets", "forward_origins", "sub_enabled", "sub_apply_delay"
        ])

        gaugeName = "select_table_replication_status"
        gaugeInfo = "Select table raplication status"
        gaugeListTablesSyncStatus = GaugeMetricFamily(gaugeName, gaugeInfo, labels=[
            "host", "database", "sync_subid" , "sync_nspname", "sync_relname", "sync_status", "sync_statuslsn"
        ])

        #Loop databases from config file
        for db in config["db_list"]:
            conn_master = psycopg2.connect(
                database=db["name"],
                host=config["src_db"]["host"],
                user=config["src_db"]["user"],
                password=config["src_db"]["pass"],
                port=config["src_db"]["port"]
            )
            conn_master.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            slaves = []
            #QUERIES
            # CREATE SLAVE LIST AND REPLICATIONS
            sql_select_pg_stat_replication = postgresql.executeQuery ( conn_master, "SELECT * FROM pg_stat_replication;" ) #take slaves from here
            for replications in sql_select_pg_stat_replication:
                dbname = str(replications[3]).split("_")
                slaves.append([replications[3], replications[4], dbname[0]] )
                gaugeReplication.add_metric([
                    "master", db["name"], str(replications[3]), str(replications[4]), str(replications[9]), str(replications[10]), str(replications[11]), str(replications[12]), str(replications[13])
                ], 1 if "streaming" in replications[9] else 0)

            #WAL SIZE ON DISK
            sql_sum_pg_ls_waldir = postgresql.executeQuery( conn_master, "SELECT sum(size) from pg_ls_waldir();" )
            gaugeLSWalDir.add_metric([
                "master", db["name"]
            ], sql_sum_pg_ls_waldir[0][0]/1024/1024)

            #MIN WAL SIZE IN CONFIG
            sql_select_min_wal_size = postgresql.executeQuery( 
                conn_master,  "SELECT setting from pg_settings where name in ('min_wal_size');" 
            )
            gaugeMinWalSize.add_metric([
                "master", db["name"]
            ], sql_select_min_wal_size[0][0])

            #MAX WAL SIZE IN CONFIG
            sql_select_max_wal_size = postgresql.executeQuery( 
                conn_master, 
                "SELECT setting from pg_settings where name in ('max_wal_size');" 
            )
            gaugeMaxWalSize.add_metric([
                "master", db["name"]
            ], sql_select_max_wal_size[0][0])

            for slave in slaves:
                conn_slave = psycopg2.connect(
                    database=slave[2],
                    host=slave[1],
                    user="postgres",
                    password="test1234",
                    port="5432"
                )
                sql_sum_pg_ls_waldir = postgresql.executeQuery( conn_slave, "SELECT sum(size) from pg_ls_waldir();" )
                gaugeLSWalDir.add_metric([
                    str(slave[1]), db["name"]
                ], sql_sum_pg_ls_waldir[0][0]/1024/1024)

                sql_select_min_wal_size = postgresql.executeQuery( 
                    conn_slave,  "SELECT setting from pg_settings where name in ('min_wal_size');" 
                )
                gaugeMinWalSize.add_metric([
                    str(slave[1]), db["name"]
                ], sql_select_min_wal_size[0][0])

                sql_select_max_wal_size = postgresql.executeQuery( 
                    conn_slave, "SELECT setting from pg_settings where name in ('max_wal_size');" 
                )
                gaugeMaxWalSize.add_metric([
                    str(slave[1]), db["name"]
                ], sql_select_max_wal_size[0][0])


            #REPLICATION SLOTS
            sql_select_pg_replication_slots = postgresql.executeQuery( conn_master, "SELECT * FROM pg_replication_slots;" )
            for slot in sql_select_pg_replication_slots:
                gaugeActiveReplicationSlots.add_metric([
                    "master", db["name"], str(slot[0]), str(slot[4]), str(slot[10]), str(slot[11]), str(slot[12]), str(slot[13])
                ], 1 if "True" in str(slot[6]) else 0 )
                gaugeSlotsWal.add_metric([
                    "master", db["name"], str(slot[0]), str(slot[4]), str(slot[10]), str(slot[11]), str(slot[12])
                ], 0 if slot[13] is None else str(slot[13]) )

            sql_show_max_replication_slots = postgresql.executeQuery( conn_master, "SHOW max_replication_slots;" )
            gaugeTotalReplicationSlots.add_metric([
                "master", db["name"]
            ], sql_show_max_replication_slots[0][0])


            # #LIST ACTIVE PG LOGICAL NODES
            sql_select_pglogical_node = postgresql.executeQuery ( conn_master, "SELECT * FROM pglogical.node;" )
            for node in sql_select_pglogical_node:
                gaugeListNodes.add_metric([
                    "master", db["name"], str(node[0]), str(node[1])
                ], 1)
            #loop slaves
            for slave in slaves:
                conn_slave = psycopg2.connect(
                    database=slave[2],
                    host=slave[1],
                    user="postgres",
                    password="test1234",
                    port="5432"
                )
                conn_slave.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                sql_select_pglogical_node_slave = postgresql.executeQuery ( conn_slave, "SELECT * FROM pglogical.node;" )
                for node in sql_select_pglogical_node_slave:
                    gaugeListNodes.add_metric([
                        str(slave[1]), db["name"], str(node[0]), str(node[1])
                    ],1)


            # SELECT REPLICATION SETS
            sql_select_pg_replication_set = postgresql.executeQuery ( conn_master, "SELECT * FROM pglogical.replication_set;" )
            for repSet in sql_select_pg_replication_set:
                gaugeListReplicationSets.add_metric([
                    "master", db["name"], str(repSet[0]), str(repSet[1]), str(repSet[2]), str(repSet[3]), str(repSet[4]), str(repSet[5]), str(repSet[6])
                ], 1)

            # SELECT SUBSCRIPTION SETS AND STATUS FROM SLAVES
            for slave in slaves:
                conn_slave = psycopg2.connect(
                    database=slave[2],
                    host=slave[1],
                    user="postgres",
                    password="test1234",
                    port="5432"
                )
                conn_slave.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                subscrioptions = {}
                sql_select_pg_show_subscription_status = postgresql.executeQuery ( conn_slave, "SELECT * from pglogical.show_subscription_status();" )
                for SUBS in sql_select_pg_show_subscription_status:
                    subscrioptions[SUBS[0]] = [SUBS[0], SUBS[1], SUBS[2], SUBS[3], SUBS[4], SUBS[5], SUBS[6], 0, 0]
                sql_select_pg_replication_subscriptions = postgresql.executeQuery ( conn_slave, "SELECT * from pglogical.subscription;" )
                for SUBS in sql_select_pg_replication_subscriptions:
                    subscrioptions[SUBS[1]][len(subscrioptions[SUBS[1]])-2] = SUBS[6]
                    subscrioptions[SUBS[1]][len(subscrioptions[SUBS[1]])-1] = str(SUBS[10])
                for subName, subValue in subscrioptions.items():
                    gaugeListReplicationSubscriptions.add_metric([
                        str(slave[1]), db["name"], str(subValue[0]), str(subValue[1]), str(subValue[2]), str(subValue[3]), str(subValue[4]), str(subValue[5]), str(subValue[6]), str(subValue[7]), str(subValue[8])
                    ], 1 if "replicating" in str(subValue[1]) else 0)

            # SELECT TABLES AND SYNC STATUS
            for slave in slaves:
                conn_slave = psycopg2.connect(
                    database=slave[2] ,
                    host=slave[1],
                    user="postgres",
                    password="test1234",
                    port="5432"
                )
                conn_slave.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                subscrioptions = {}
                sql_select_pg_show_subscription_status = postgresql.executeQuery ( conn_slave, "SELECT * from pglogical.local_sync_status;" )
                for table in sql_select_pg_show_subscription_status:
                    gaugeListTablesSyncStatus.add_metric([
                        str(slave[1]), db["name"], str(table[1]), str(table[2]), str(table[3]), str(table[4]), str(table[5])
                    ], 1 if "r" in str(table[4]) else 0)
            
            yield gaugeLSWalDir
            yield gaugeMinWalSize
            yield gaugeMaxWalSize
            yield gaugeReplication
            yield gaugeActiveReplicationSlots
            yield gaugeSlotsWal
            yield gaugeTotalReplicationSlots
            yield gaugeListNodes
            yield gaugeListReplicationSets
            yield gaugeListReplicationSubscriptions
            yield gaugeListTablesSyncStatus


if __name__ == "__main__":
    start_http_server(config["exporter"]["port"])
    REGISTRY.register(PGLogicalCollector())
    while True: 
        # period between collection
        print ("---------------------------------")
        time.sleep(config["exporter"]['scrape_frequency'])