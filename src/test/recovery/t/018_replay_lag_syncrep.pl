# This test demonstrates that synchronous replication is affected by
# replay process on standby.  If the replay process lags far behind
# and the replication connection is broken (e.g. temporary network
# problem) the connection is not established again until the replay
# process finishes replaying all WAL.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 5;

# Query checking sync_priority and sync_state of each standby
my $check_sql =
  "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication ORDER BY application_name;";

# Check that sync_state of a standby is expected (waiting till it is).
# If $setting is given, synchronous_standby_names is set to it and
# the configuration file is reloaded before the test.
sub test_sync_state
{
	my ($self, $expected, $msg, $setting) = @_;

	if (defined($setting))
	{
		$self->safe_psql('postgres',
						 "ALTER SYSTEM SET synchronous_standby_names = '$setting';");
		$self->reload;
	}

	ok($self->poll_query_until('postgres', $check_sql, $expected), $msg);
	return;
}

# Start a standby and check that it is registered within the WAL sender
# array of the given primary.  This polls the primary's pg_stat_replication
# until the standby is confirmed as registered.
sub start_standby_and_wait
{
	my ($master, $standby) = @_;
	my $master_name  = $master->name;
	my $standby_name = $standby->name;
	my $query =
	  "SELECT count(1) = 1 FROM pg_stat_replication WHERE application_name = '$standby_name'";

	$standby->start;

	print("### Waiting for standby \"$standby_name\" on \"$master_name\"\n");
	$master->poll_query_until('postgres', $query);
	return;
}

# Initialize master node
my $node_master = get_new_node('master');
my @extra = (q[--wal-segsize], q[1]);
$node_master->init(allows_streaming => 1, extra => \@extra);
$node_master->start;
my $backup_name = 'master_backup';

# Setup physical replication slot for streaming replication
$node_master->safe_psql('postgres',
	q[SELECT pg_create_physical_replication_slot('phys_slot', true, false);]);

# Take backup
$node_master->backup($backup_name);

# Create standby linking to master
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, $backup_name,
								has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
						   q[primary_slot_name = 'phys_slot']);
# Enable debug logging in standby
$node_standby->append_conf('postgresql.conf',
						   q[log_min_messages = debug5]);

start_standby_and_wait($node_master, $node_standby);

# Make standby synchronous
test_sync_state(
	$node_master,
	qq(standby|1|sync),
	'standby is synchronous',
	'standby');

# Slow down WAL replay by inducing 10 seconds sleep before replaying
# each WAL record.
$node_standby->safe_psql('postgres', 'ALTER SYSTEM set debug_replay_delay TO 10;');
$node_standby->reload;

# Load data on master and induce replay lag in standby.
$node_master->safe_psql('postgres', 'CREATE TABLE replay_lag_test(a int);');
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test select i from generate_series(1,100) i;');

# Obtain WAL sender PID and kill it.
my $walsender_pid = $node_master->safe_psql(
	'postgres',
	q[select active_pid from pg_get_replication_slots() where slot_name = 'phys_slot']);

# Kill walsender, so that the replication connection breaks.
kill 'SIGTERM', $walsender_pid;

# The replication connection should be re-establised because
# postmaster will restart WAL receiver in its main loop.  Try to
# commit a transaction with a timeout of 2 seconds.  The test expects
# that the commit does not timeout.
my $timed_out = 0;
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (1);',
	timeout => 2,
	timed_out => \$timed_out);

# The insert should not timeout because synchronous replication is
# re-established, even when startup process is still replaying
# WAL already fetched in pg_wal/.
is($timed_out, 0, 'insert after WAL receiver restart');

# Break the replication connection by restarting standby.
$node_standby->restart;

# The replication connection should be re-establised by upon standby
# restart.  Try to commit a transaction with a 2 second timeout.  The
# timeout should not be hit because synchronous replication should be
# re-established before startup process finishes the replay of WAL
# already available in pg_wal/.
$timed_out = 0;
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (2);',
	timeout => 1,
	timed_out => \$timed_out);

# Reset the debug GUC, so that the replay process is no longer slowed down.
$node_standby->safe_psql('postgres', 'ALTER SYSTEM set debug_replay_delay TO 0;');
$node_standby->reload;

# Ideally, the insert after standby restart should not
# timeout but it currently does, causing the test to fail.
is($timed_out, 0, 'insert after standby restart');

# Switch to a new WAL file and see if things work well.
$node_master->safe_psql(
	'postgres',
	'select pg_switch_wal();');

# Transactions should work fine on master.
$timed_out = 0;
$node_master->safe_psql(
	'postgres',
	'insert into replay_lag_test values (3);',
	timeout => 1,
	timed_out => \$timed_out);

# Standby should also have identical content, now that we've reset the
# replay delay.
my $count_sql = q[select count(*) from replay_lag_test;];
my $expected = q[103];
ok($node_standby->poll_query_until('postgres', $count_sql, $expected), 'standby query');

$node_standby->promote;
$node_master->stop;
$node_standby->safe_psql('postgres', 'insert into replay_lag_test values (4);');

$expected = q[104];
ok($node_standby->poll_query_until('postgres', $count_sql, $expected),
   'standby query after promotion');
