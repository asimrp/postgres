-- Validate that replay lag does not affect synchronous replication

create extension if not exists faultinjector;

create table replay_lag_test(note varchar, ts timestamp with time zone);

-- Induce replay lag by injecting a fault to sleep for 10 seconds in
-- the WAL replay loop of standby server.  The _infinite suffix
-- indicates that fault will keep triggering until it is reset.
select inject_fault_remote('redo_main_loop', 'sleep',
       		'', '', 1, -1, 10, 'localhost', 5433);

-- Setup synchronous replication, so that commits on primary wait
-- until standby confirms WAL flush up to commit LSN.
alter system set synchronous_standby_names to '*';
select pg_reload_conf();
show synchronous_standby_names;

-- Generate some WAL on master
insert into replay_lag_test values ('before replay lag', now());
insert into replay_lag_test values ('before replay lag', now());

-- Check if replay is lagging by sufficient amount
select flush_lsn > replay_lsn as replay_is_lagging from pg_stat_replication;

-- Inject fault in WAL receiver's main loop so that the WAL receiver
-- process exits with FATAL, causing replication connection to get
-- disconnected.
select inject_fault('wal_receiver_loop', 'fatal', 'localhost', 5433);

select wait_until_triggered_fault('wal_receiver_loop', 1, 'localhost', 5433);

select * from pg_stat_replication;

-- This should wait until standby confirms flush up to commit LSN.
insert into replay_lag_test values ('after replay lag', now());

-- Check that the wait time was no longer than 10 seconds (the sleep
-- fault time).
select now() - ts < '00:00:10' as passed from replay_lag_test
where note = 'after replay lag';

select inject_fault('all', 'reset', 'localhost', 5433);
