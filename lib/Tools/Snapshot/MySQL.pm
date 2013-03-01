package MMM::Tools::Snapshot::MySQL;

use strict;
use warnings FATAL => 'all';
use Data::Dumper;
use DBI;
use Log::Log4perl qw(:easy);


sub lock_tables($) {
	my $dbh	= shift;

	INFO 'Locking tables...';
	my $res = $dbh->do('FLUSH TABLES WITH READ LOCK');
	INFO "Result: '$res'";

	system('sync');
	sleep(1);
	system('sync');

	return $res;
	
}

sub unlock_tables($) {
	my $dbh	= shift;
	return $dbh->do('UNLOCK TABLES');
}

sub get_pos_info($$) {
	my $dbh = shift;
	my $pos_info = shift;
	
	# Get master status info
	my $res = $dbh->selectrow_hashref('SHOW MASTER STATUS');
	return "ERROR: Can't get master status information! Error: " . $dbh->errstr unless ($res);
	$pos_info->{master} = $res;

	# Get slave status info
	$res = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
	return "ERROR: Can't get slave status information! Error: " . $dbh->errstr if (defined($dbh->err));
	$res = {} unless ($res);
	$pos_info->{slave} = $res;

	return 'OK: Got status info!';
}


sub save_pos_info($$) {
	my $pos_info	= shift;
	my $file		= shift;
	
	open(POSFILE, '>', $file) || return "ERROR: Can't create pos file: $!";
	print POSFILE Dumper($pos_info);
	close(POSFILE);
	
	return 'OK: Saved position info';
}

=item _get_connection_info($host)

Get connection info for host $host

=cut

sub get_connection_info($) {
	my $host = shift;

	# TODO maybe check $host

	return (
		$main::config->{host}->{$host}->{ip},
		$main::config->{host}->{$host}->{mysql_port},
		$main::config->{host}->{$host}->{tools_user},
		$main::config->{host}->{$host}->{tools_password}
	);
}

sub connect($) {
	my $host_name = shift;
	my ($host, $port, $user, $password)	= get_connection_info($host_name);
	my $dsn = "DBI:mysql:host=$host;port=$port;mysql_connect_timeout=3";
	return DBI->connect($dsn, $user, $password, { PrintError => 0 });
}

1;
