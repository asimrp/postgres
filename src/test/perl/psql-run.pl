use strict;
use warnings;

use IPC::Run qw( start pump finish );


=pod

=item startpsql

Starts a psql subprocess using IPC::Run::start interface.  The harness
object, input, output and error descriptors of the psql child process
are returned as a hash.

=cut
sub startpsql
{
	my ($host, $port) = @_;
	my %ret;
	my $in;
	my $out;
	my $err;
	my $harness;
	my @psql;
	unless (defined($host) && defined($port))
	{
		die "host name and port number not specified";
	}
	@psql = qw( psql -d postgres -h );
	$psql[++$#psql] = $host;
	$psql[++$#psql] = '-p';
	$psql[++$#psql] = $port;

	print " ".join(@psql);

	$ret{"harness"} = start \@psql, \$in, \$out, \$err;
	$ret{"in"} = \$in;
	$ret{"out"} = \$out;
	$ret{"err"} = \$err;
	return \%ret;
}

=pod

=item sendSQL

Sends a SQL statement to a session that was created with startpsql().
The function returns immediately after the SQL string is sent across
to the child psql process, without waiting for results.  Call
getResults() to obtain results.

=cut
sub sendSQL
{
	my $session = $_[0];
	my $outref = $session->{out};
	my $errref = $session->{err};

	# Reset output and error buffers so that they will only contain
	# the results of this SQL command.
	$$outref = "";
	$$errref = "";

	# Assigning the SQL statement to $inref causes it to be sent to
	# the psql child process.
	my $inref = $session->{in};
	$$inref = $_[1];

	pump $session->{harness} while length $$inref;
}

=pod

=item getResults

Blocks until results of previously sent SQL statement are available
from a SQL session created using startsql().  Accepts a regex
representing expected output.  The function will return as soon as a
line matching the regex is found in the output.

   getResults($session, "([0-9]* row[s])")

=cut
sub getResults
{
	my $session = $_[0];
	my $inref = $session->{in};
	my $outref = $session->{out};
	my $errref = $session->{err};

	while ($$outref !~ /$_[1]/ && $$errref !~ /ERR/)
	{
		pump $session->{harness};
	}
	die "psql failed:\n", $$errref if length $$errref;
	return $$outref;
}

my $s1 = startpsql('localhost', 5432);
my $s2 = startpsql('localhost', 5432);

sendSQL $s1, "truncate testme\n; select inject_fault('all', 'reset');\n";
sendSQL $s2, "insert into testme values (1), (2);\n";
print (getResults $s1, "([0-9]* row[s]?)");
print (getResults $s2, "INSERT");

sendSQL $s2, "select * from testme;\n";
print (getResults $s2, "([0-9]+ rows)");

finish $s1->{harness};
finish $s2->{harness};
