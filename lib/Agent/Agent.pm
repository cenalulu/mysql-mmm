package MMM::Agent::Agent;

use strict;
use warnings FATAL => 'all';
use English qw(EVAL_ERROR);
use Algorithm::Diff;
use DBI;
use Class::Struct;
use Errno qw(EINTR);
use Log::Log4perl qw(:easy);
use MMM::Common::Role;
use MMM::Common::Socket;
use MMM::Agent::Helpers;
use MMM::Agent::Role;

eval {
    no warnings 'once';
    require Unix::Uptime;
    *uptime = *Unix::Uptime->uptime;
};
if ($EVAL_ERROR) {
	require MMM::Common::Uptime;
	MMM::Common::Uptime->import(qw(uptime));
}

our $VERSION = '0.01';


struct 'MMM::Agent::Agent' => {
	name				=> '$',
	ip					=> '$',
	port				=> '$',
	interface			=> '$',
	mode				=> '$',
	mysql_port			=> '$',
	mysql_user			=> '$',
	mysql_password		=> '$',
	writer_role			=> '$',
	bin_path			=> '$',

	active_master		=> '$',
	state				=> '$',
	roles				=> '@',
	config_file			=> '$'
};

sub main($) {
	my $self	= shift;
	my $socket	= MMM::Common::Socket::create_listener($self->ip, $self->port);
	$self->roles([]);
	$self->active_master('');

	while (!$main::shutdown) {

		DEBUG 'Listener: Waiting for connection...';
		my $client = $socket->accept();
		next unless ($client);

		DEBUG 'Listener: Connect!';
		while (my $cmd = <$client>) {
			chomp($cmd);
			DEBUG "Daemon: Command = '$cmd'";

			my $res = $self->handle_command($cmd);
			my $uptime = uptime();

			print $client "$res|UP:$uptime\n";
			DEBUG "Daemon: Answer = '$res'";

			return 0 if ($main::shutdown);
		}

		close($client);
		DEBUG 'Listener: Disconnect!';
		
		$self->check_roles();
	}
}

sub handle_command($$) {
	my $self	= shift;
	my $cmd		= shift;

	DEBUG "Received Command $cmd";
	my ($cmd_name, $version, $host, @params) = split('\|', $cmd, -1);

	return "ERROR: Invalid command '$cmd'!" unless (defined($host));
	return "ERROR: Invalid hostname in command ($host)! My name is '" . $self->name . "'" if ($host ne $self->name);
	
	if ($version > main::MMM_PROTOCOL_VERSION) {
		WARN "Version in command '$cmd_name' ($version) is greater than mine (", main::MMM_PROTOCOL_VERSION, ")"
	}
	
	if		($cmd_name eq 'PING')				{ return cmd_ping						();			}
	elsif	($cmd_name eq 'SET_STATUS')			{ return $self->cmd_set_status			(@params);	}
	elsif	($cmd_name eq 'GET_AGENT_STATUS')	{ return $self->cmd_get_agent_status	();			}
	elsif	($cmd_name eq 'GET_SYSTEM_STATUS')	{ return $self->cmd_get_system_status	();			}
	elsif	($cmd_name eq 'CLEAR_BAD_ROLES')	{ return $self->cmd_clear_bad_roles		();			}

	return "ERROR: Invalid command '$cmd_name'!";
}

sub cmd_ping() {
	return 'OK: Pinged!';
}

sub cmd_get_agent_status($) {
	my $self	= shift;
	
	my $answer = join ('|', (
		$self->state,
		join(',', @{$self->roles}),
		$self->active_master
	));
	return "OK: Returning status!|$answer";
}

sub cmd_get_system_status($) {
	my $self	= shift;

	# determine master info
    my $dsn			= sprintf("DBI:mysql:host=%s;port=%s;mysql_connect_timeout=3", $self->ip, $self->mysql_port);
    my $eintr		= EINTR;
	my $master_ip	= '';

    my $dbh;
CONNECT: {
    DEBUG "Connecting to mysql";
    $dbh   = DBI->connect($dsn, $self->mysql_user, $self->mysql_password, { PrintError => 0 });
    unless ($dbh) {
        redo CONNECT if ($DBI::err == 2003 && $DBI::errstr =~ /\($eintr\)/);
        WARN "Couldn't connect to mysql. Can't determine current master host." . $DBI::err . " " . $DBI::errstr;
    }
}

    my $slave_status = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
	$master_ip = $slave_status->{Master_Host} if (defined($slave_status));

	my @roles;
	foreach my $role (keys(%{$main::config->{role}})) {
		my $role_info = $main::config->{role}->{$role};
		foreach my $ip (@{$role_info->{ips}}) {
			my $res = MMM::Agent::Helpers::check_ip($self->interface, $ip);
			my $ret = $? >> 8;
			return "ERROR: Could not check if IP is configured: $res" if ($ret == 255);
			next unless ($ret == 0);
			# IP is configured...
			push @roles, new MMM::Common::Role::(name => $role, ip => $ip);
		}
	}
	my $res = MMM::Agent::Helpers::may_write();
	my $ret = $? >> 8;
	return "ERROR: Could not check if MySQL is writable: $res" if ($ret == 255);
	my $writable = ($ret == 1);

	my $answer = join('|', ($writable, join(',', @roles), $master_ip));
	return "OK: Returning status!|$answer";
}

sub cmd_clear_bad_roles($) {
	my $self	= shift;
	my $count	= 0;
	foreach my $role (keys(%{$main::config->{role}})) {
		my $role_info = $main::config->{role}->{$role};
		foreach my $ip (@{$role_info->{ips}}) {
			my $role_valid = 0;
			foreach my $agentrole (@{$self->roles}) {
				next unless ($agentrole->name eq $role);
				next unless ($agentrole->ip   eq $ip);
				$role_valid = 1;
				last;
			}
			next if ($role_valid);
			my $res = MMM::Agent::Helpers::check_ip($self->interface, $ip);
			my $ret = $? >> 8;
			return "ERROR: Could not check if IP is configured: $res" if ($ret == 255);
			next if ($ret == 1);
			# IP is configured...
			my $roleobj = new MMM::Agent::Role::(name => $role, ip => $ip);
			$roleobj->del();
			$count++;
		}
	}
	return "OK: Removed $count roles";
}

sub cmd_set_status($$) {
	my $self	= shift;
	my ($new_state, $new_roles_str, $new_master) = @_;

	# Change master if we are a slave
	if ($new_master ne $self->active_master && $self->mode eq 'slave' && $new_state eq 'ONLINE' && $new_master ne '') {
		INFO "Changing active master to '$new_master'";
		my $res = MMM::Agent::Helpers::set_active_master($new_master);
		DEBUG sprintf("Result: %s", defined($res) ? $res : 'undef');
		if (defined($res) && $res =~ /^OK/) {
			$self->active_master($new_master);
		}
		else {
			FATAL sprintf("Failed to change master to '%s': %s", $new_master, defined($res) ? $res : 'undef');
		}
	}

	# Parse roles
	my @new_roles_arr = sort(split(/\,/, $new_roles_str));
	my @new_roles;
	foreach my $role_str (@new_roles_arr) {
		my $role = MMM::Agent::Role->from_string($role_str);
		if (defined($role)) {
			push @new_roles, $role;
		}
	}

	# Process roles
	my @added_roles = ();
	my @deleted_roles = ();
	my $changes_count = 0;

	# Determine changes
	my $diff = new Algorithm::Diff:: ($self->roles, \@new_roles, { keyGen => \&MMM::Common::Role::to_string });
	while ($diff->Next) {
		next if ($diff->Same);

		$changes_count++;
		push (@deleted_roles,	$diff->Items(1)) if ($diff->Items(1));
		push (@added_roles,		$diff->Items(2)) if ($diff->Items(2));
	}

	# Apply changes
	if ($changes_count) {
		INFO 'We have some new roles added or old rules deleted!';
		INFO 'Deleted: ', join(', ', sort(@deleted_roles))	if (scalar(@deleted_roles));
		INFO 'Added:   ', join(', ', sort(@added_roles))	if (scalar(@added_roles));

		foreach my $role (@deleted_roles)	{ $role->del(); }
		foreach my $role (@added_roles)		{ $role->add(); }

		$self->roles(\@new_roles);
	}
	
	# Process state change
	if ($new_state ne $self->state) {
		if ($new_state		eq 'ADMIN_OFFLINE') { MMM::Agent::Helpers::turn_off_slave();	}
		if ($self->state	eq 'ADMIN_OFFLINE') { MMM::Agent::Helpers::turn_on_slave();		}
		$self->state($new_state);
	}

	return 'OK: Status applied successfully!';
}

sub check_roles($) {
	my $self	= shift;

	foreach my $role (@{$self->roles}) {
		$role->check();	
	}
}

sub from_config($%) {
	my $self	= shift;
	my $config	= shift;

	my $host = $config->{host}->{$config->{this}};

	$self->name				($config->{this});
	$self->ip				($host->{ip});
	$self->port				($host->{agent_port});
	$self->interface		($host->{cluster_interface});
	$self->mode				($host->{mode});
	$self->mysql_port		($host->{mysql_port});
	$self->mysql_user		($host->{agent_user});
	$self->mysql_password	($host->{agent_password});
	$self->writer_role		($config->{active_master_role});
	$self->bin_path			($host->{bin_path});
}

1;
