package MMM::Monitor::Checker::Checks;

use strict;
use warnings FATAL => 'all';
use English qw(OSNAME EFFECTIVE_USER_ID);
use DBI;
use Net::Ping;
use POSIX ':signal_h';

our $VERSION = '0.01';

=head1 NAME

MMM::Monitor::Checker::Checks - functions for the B<mmm_mond> helper program B<checker>

=cut


my $fping_path;

=head1 FUNCTIONS

=over 4

=item ping($timeout, $host)

Check if the host $host is reachable.

=cut

sub ping($$) {
	my $timeout	= shift;
	my $host	= shift;

	my $ip = $main::config->{host}->{$host}->{ip};
	return "ERROR: Invalid host '$host'" unless ($ip);

	# if super user, use Net::Ping - it's faster
	if ($EFFECTIVE_USER_ID == 0) {
		my $p = Net::Ping->new('icmp');
		$p->hires();
		if ($p->ping($ip, 0.5)) {
			return 'OK';
		}
		return "ERROR: Could not ping $ip";
	}

	# Find appropriate fping version
	_determine_fping_path() unless defined($fping_path);
	unless (defined($fping_path)) {
		return "ERROR: fping is not functional - please, install your own version of fping on this server!";
	}

	my $res = `$fping_path -q -u -t 500 -C 1 $ip 2>&1`;
	return "ERROR: fping could not reach $ip" if ($res =~ /$ip.*\-$/);
	return 'OK';
}

=item ping_ip($timeout, $ip)

Check if the IP $ip is reachable.

=cut

sub ping_ip($$) {
	my $timeout	= shift;
	my $ip	= shift;

	# if super user, use Net::Ping - it's faster
	if ($EFFECTIVE_USER_ID == 0) {
		my $p = Net::Ping->new('icmp');
		$p->hires();
		if ($p->ping($ip, 0.5)) {
			return 'OK';
		}
		return "ERROR: Could not ping $ip";
	}

	# Find appropriate fping version
	_determine_fping_path() unless defined($fping_path);
	unless (defined($fping_path)) {
		return "ERROR: fping is not functional - please, install your own version of fping on this server!";
	}

	my $res = `$fping_path -q -u -t 500 -C 1 $ip 2>&1`;
	return "ERROR: fping could not reach $ip" if ($res =~ /$ip.*\-$/);
	return 'OK';
}


=item mysql($timeout, $host)

Check if the mysql server on host $host is reachable.

=cut

sub mysql($$) {
	my $timeout	= shift;
	my $host	= shift;

	my ($peer_host, $peer_port, $peer_user, $peer_password) = _get_connection_info($host);
	return "ERROR: Invalid host '$host'" unless ($peer_host);

	my $mask = POSIX::SigSet->new( SIGALRM );
	my $action = POSIX::SigAction->new(
		sub { die 'TIMEOUT'; },
		$mask,
	);
	my $oldaction = POSIX::SigAction->new();
	sigaction( SIGALRM, $action, $oldaction );

	my $res = eval {
		alarm($timeout + 1);
		
		# connect to server
		my $dsn = "DBI:mysql:host=$peer_host;port=$peer_port;mysql_connect_timeout=$timeout";
		my $dbh = DBI->connect($dsn, $peer_user, $peer_password, { PrintError => 0 });
		
		unless ($dbh) {
			alarm(0);
			# We don't want to trigger any action because of a simple 'too many connections' error
			return "UNKNOWN: Too many connections! " . $DBI::errstr if ($DBI::err == 1040);
			return "ERROR: Connect error (host = $peer_host:$peer_port, user = $peer_user)! " . $DBI::errstr;
		}
	
		# Check server (simple)
		my $res = $dbh->do('SELECT NOW()');
		unless ($res) {
			alarm(0);
			return 'ERROR: SQL Query Error: ' . $dbh->errstr;
		}

		alarm(1);
		$dbh->disconnect();
		$dbh = undef;

		alarm(0);
		return 0;
	};	
	alarm(0);

	return $res if ($res);
	return 'ERROR: Timeout' if ($@ =~ /^TIMEOUT/);
	return "UNKNOWN: Error occurred: $@" if $@;
	return 'OK';	

}


=item rep_backlog($timeout, $host)

Check the replication backlog on host $host.

=cut

sub rep_backlog($$) {
	my $timeout	= shift;
	my $host	= shift;

	my ($peer_host, $peer_port, $peer_user, $peer_password) = _get_connection_info($host);
	return "ERROR: Invalid host '$host'" unless ($peer_host);

	my $mask = POSIX::SigSet->new( SIGALRM );
	my $action = POSIX::SigAction->new(
		sub { die 'TIMEOUT'; },
		$mask,
	);
	my $oldaction = POSIX::SigAction->new();
	sigaction( SIGALRM, $action, $oldaction );

	my $res = eval {
		alarm($timeout + 1);
	
		# connect to server
		my $dsn = "DBI:mysql:host=$peer_host;port=$peer_port;mysql_connect_timeout=$timeout";
		my $dbh = DBI->connect($dsn, $peer_user, $peer_password, { PrintError => 0 });
		unless ($dbh) {
			alarm(0);
			return "UNKNOWN: Connect error (host = $peer_host:$peer_port, user = $peer_user)! " . $DBI::errstr;
		}
	
		# Check server (replication backlog)
		my $sth = $dbh->prepare('SHOW SLAVE STATUS');
		my $res = $sth->execute;

		if ($dbh->err) {
			alarm(1);
			my $ret = 'UNKNOWN: Unknown state. Execute error: ' . $dbh->errstr;
			$ret = "ERROR: The monitor user '$peer_user' doesn't have the required REPLICATION CLIENT privilege! " . $dbh->errstr if ($dbh->err == 1227);
			$sth->finish();
			$dbh->disconnect();
			$dbh = undef;
			alarm(0);
			return $ret;
		}

		unless ($res) {
			alarm(1);
			$sth->finish();
			$dbh->disconnect();
			$dbh = undef;
			alarm(0);
			return 'ERROR: Replication is not running';
		}
	
		my $status = $sth->fetchrow_hashref;
		alarm(1);
		$sth->finish();
		$dbh->disconnect();
		$dbh = undef;
		alarm(0);

		return 'ERROR: Replication is not set up' unless defined($status);

	
		# Check backlog size
		my $backlog = $status->{Seconds_Behind_Master};
		$backlog = 0 unless ($backlog);

		return 'OK: Backlog is null' if ($backlog == 0);
		return 'ERROR: Backlog is too big' if ($backlog > $main::check->{max_backlog});
		return 0;
	};
	alarm(0);

	return $res if ($res);
	return 'ERROR: Timeout' if ($@ =~ /^TIMEOUT/);
	return "UNKNOWN: Error occurred: $@" if $@;
	return 'OK';
}


=item rep_threads($timeout, $host)

Check if the mysql slave threads on host $host are running.

=cut

sub rep_threads($$) {
	my $timeout	= shift;
	my $host	= shift;

	my ($peer_host, $peer_port, $peer_user, $peer_password) = _get_connection_info($host);
	return "ERROR: Invalid host '$host'" unless ($peer_host);

	my $mask = POSIX::SigSet->new( SIGALRM );
	my $action = POSIX::SigAction->new(
		sub { die 'TIMEOUT'; },
		$mask,
	);
	my $oldaction = POSIX::SigAction->new();
	sigaction( SIGALRM, $action, $oldaction );

	my $res = eval {
		alarm($timeout + 1);
	
		# connect to server
		my $dsn = "DBI:mysql:host=$peer_host;port=$peer_port;mysql_connect_timeout=$timeout";
		my $dbh = DBI->connect($dsn, $peer_user, $peer_password, { PrintError => 0 });
		return "UNKNOWN: Connect error (host = $peer_host:$peer_port, user = $peer_user)! " . $DBI::errstr unless ($dbh);
	
		# Check server (replication backlog)
		my $sth = $dbh->prepare('SHOW SLAVE STATUS');
		my $res = $sth->execute;

		if ($dbh->err) {
			alarm(1);
			my $ret = 'UNKNOWN: Unknown state. Execute error: ' . $dbh->errstr;
			$ret = "ERROR: The monitor user '$peer_user' doesn't have the required REPLICATION CLIENT privilege! " . $dbh->errstr if ($dbh->err == 1227);
			$sth->finish();
			$dbh->disconnect();
			$dbh = undef;
			alarm(0);
			return $ret;
		}

		unless ($res) {
			alarm(1);
			$sth->finish();
			$dbh->disconnect();
			$dbh = undef;
			alarm(0);
			return 'ERROR: Replication is not running';
		}
	
		my $status = $sth->fetchrow_hashref;

		alarm(1);
		$sth->finish();
		$dbh->disconnect();
		$dbh = undef;
		alarm(0);

		return 'ERROR: Replication is not set up' unless defined($status);

		# Check peer replication state
		if ($status->{Slave_IO_Running} eq 'No' || $status->{Slave_SQL_Running} eq 'No') {
			return 'ERROR: Replication is broken';
		}
		return 0;
	};
	alarm(0);

	return $res if ($res);
	return 'ERROR: Timeout' if ($@ =~ /^TIMEOUT/);
	return "UNKNOWN: Error occurred: $@" if $@;
	return 'OK';
}


=item _get_connection_info($host)

Get connection info for host $host.

=cut

sub _get_connection_info($) {
	my $host = shift;
	return (
		$main::config->{host}->{$host}->{ip},
		$main::config->{host}->{$host}->{mysql_port},
		$main::config->{host}->{$host}->{monitor_user},
		$main::config->{host}->{$host}->{monitor_password}
	);
}


=item _determine_fping_path()

Determine path of fping binary

=cut

sub _determine_fping_path() {
	$fping_path = _determine_path('fping');
}

sub _determine_path($) {
	my $program = shift;
	my @paths = qw(/usr/sbin /sbin /usr/bin /bin);

	foreach my $path (@paths) {
		my $fullpath = "$path/$program";
		if (-f $fullpath && -x $fullpath) {
			return $fullpath;
		}
	}
	return undef;
}

1;
