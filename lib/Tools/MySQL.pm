package MMM::Tools::MySQL;

use strict;
use warnings FATAL => 'all';
use DBI;
use Log::Log4perl qw(:easy);


our $VERSION = '0.01';

=head1 NAME

MMM::Tools::MySQL - MySQL related functions for the mmm-tools.

=cut


=over 4

=item is_running([$pid])

Check if mysqld is running

=cut

sub is_running {

	my $pid = shift;

	unless ($pid) {
		my $pidfile = $main::config->{host}->{$main::config->{this}}->{mysql_pidfile};
		return 0 unless (-f $pidfile);
		open(PID, $pidfile) || LOGDIE "ERROR: Can't read MySQL pid file '$pidfile'";
		chomp($pid = <PID>);
		close(PID);
	}

	my $cnt = kill(0, $pid);
	return $pid if ($cnt);
	return 0;
}


=item start

Start mysqld

=cut

sub start() {
	my $rcscript = $main::config->{host}->{$main::config->{this}}->{mysql_rcscript};

	my $pid = is_running();
	if ($pid) {
		ERROR "Error: Local MySQL server is running with pid $pid";
		return 0;
	}

	INFO 'MySQL is not running. Going to start it...';

	my $res = system($rcscript, 'start');
	if ($res) {
		ERROR "ERROR: Can't start local MySQL server!";
		return 0;
	}

	INFO 'MySQL has been started!';
	return 1;
}

=item stop

Stop mysqld

=cut

sub stop() {
	my $rcscript = $main::config->{host}->{$main::config->{this}}->{mysql_rcscript};

	my $pid = is_running();
	unless ($pid) {
		WARN 'MySQL is not running now, skipping shutdown ...';
		return 1;
	}

	my $res = system($rcscript, 'stop');
	if ($res) {
		ERROR "ERROR: Can't stop local MySQL server!";
		return 0;
	}

	my $wait = 15;
	DEBUG ("Waiting MySQL process with $pid to shutdown: ");
	while ($wait--) {
		$pid = is_running($pid);
		last if ($pid == 0);
		DEBUG '.';
		sleep(1);
	}

	if ($pid != 0) {
		ERROR "ERROR: MySQL is running with PID $pid after shutdown request!";
		return 0;
	}

	INFO 'MySQL has been stopped!';
	return 1;
}

=item change_master_to(named_params)

Required params
	master_host
	master_port
	master_user
	master_pass

Optional params
	master_log
	master_pos

=cut
sub change_master_to {
	my $args = shift;

	$args->{host} ||= $main::config->{this};


	LOGDIE 'Bad call of change_master_to()' unless (
		defined($args->{master_host}) && defined($args->{master_port})
	 && defined($args->{master_user}) && defined($args->{master_pass})
	);

	INFO "Changing master of host $args->{host} to $args->{master_host} ...";

	# Get connection information
	my ($host, $port, $user, $password)	= _get_connection_info($args->{host});
	unless (defined($host)) {
		ERROR "No connection info for host '$args->{host}'";
		return 0;
	}

	# Connect to server
	my $dbh = _connect($host, $port, $user, $password);
	unless ($dbh) {
		ERROR "Can't connect to MySQL (host = $host:$port, user = $user)!";
		return 0;
	}


	my $res;

	# Stop slave
	$res = $dbh->do('STOP SLAVE');
	unless ($res) {
		ERROR 'SQL Query Error: ', $dbh->errstr;
		return 0;
	}

	# Force deletion of obsolete master.info, relay-log.info and relay logs.
	$res = $dbh->do('RESET SLAVE');
	unless ($res) {
		ERROR 'SQL Query Error: ', $dbh->errstr;
		return 0;
	}

	# Change master
	my $sql = sprintf(
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%s, MASTER_USER='%s', MASTER_PASSWORD='%s'",
		$args->{master_host}, $args->{master_port}, $args->{master_user}, $args->{master_pass}
	);

	if ($args->{master_log} && $args->{master_pos}) {
		$sql .= sprintf(", MASTER_LOG_FILE='%s', MASTER_LOG_POS=%s", $args->{master_log}, $args->{master_pos});
	}
	
	$res = $dbh->do($sql);
	unless ($res) {
		ERROR 'SQL Query Error: ', $dbh->errstr;
		return 0;
	}

	# Start slave
	$res = $dbh->do('START SLAVE');
	unless ($res) {
		ERROR 'SQL Query Error: ', $dbh->errstr;
		return 0;
	}
	
	# Disconnect
	$dbh->disconnect;

	INFO "Successfully changed master.";

	return 1;
}

=item _get_connection_info($host)

Get connection info for host $host

=cut

sub _get_connection_info($) {
	my $host = shift;

	# TODO maybe check $host

	return (
		$main::config->{host}->{$host}->{ip},
		$main::config->{host}->{$host}->{mysql_port},
		$main::config->{host}->{$host}->{tools_user},
		$main::config->{host}->{$host}->{tools_password}
	);
}

sub _connect($$$$) {
	my ($host, $port, $user, $password)	= @_;
	my $dsn = "DBI:mysql:host=$host;port=$port;mysql_connect_timeout=3";
	return DBI->connect($dsn, $user, $password, { PrintError => 0 });
}

sub get_master_host($) {
	my $host_name = shift;

	# Get connection information
	my ($host, $port, $user, $password)	= _get_connection_info($host_name);
	unless (defined($host)) {
		ERROR "No connection info for host '$host_name'";
		return undef;
	}

	# Connect to server
	my $dbh = _connect($host, $port, $user, $password);
	unless ($dbh) {
		ERROR "Can't connect to MySQL (host = $host:$port, user = $user)!";
		return undef;
	}

	# Get slave status
	my $res = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
	return "ERROR: Can't get slave status for host '$host_name'! Error: " . $dbh->errstr unless ($res);

	# Disconnect
	$dbh->disconnect();	

	return $res->{Master_Host};
}

1;
