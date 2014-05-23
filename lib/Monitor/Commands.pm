package MMM::Monitor::Commands;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use List::Util qw(max);
use threads;
use threads::shared;
use Log::Log4perl::DateFormat;
use MMM::Common::Socket;
use MMM::Monitor::Agents;
use MMM::Monitor::ChecksStatus;
use MMM::Monitor::Monitor;
use MMM::Monitor::Roles;

our $VERSION = '0.01';


sub main($$) {
	my $queue_in	= shift;
	my $queue_out	= shift;

	my $socket	= MMM::Common::Socket::create_listener($main::config->{monitor}->{ip}, $main::config->{monitor}->{port});

	while (!$main::shutdown) {
		DEBUG 'Listener: Waiting for connection...';
		my $client = $socket->accept();
		next unless ($client);

		DEBUG 'Listener: Connect!';
		while (my $cmd = <$client>) {	
			chomp($cmd);
			last if ($cmd eq 'quit');

			$queue_out->enqueue($cmd);
			my $res;
			until ($res) {
				lock($queue_in);
				cond_timedwait($queue_in, time() + 1); 
				$res = $queue_in->dequeue_nb();
				return 0 if ($main::shutdown);
			}
			print $client $res;
			return 0 if ($main::shutdown);
		}

		close($client);
		DEBUG 'Listener: Disconnect!';

	}	
}

sub ping() {
	return 'OK: Pinged successfully!';
}


sub show() {
	my $agents	= MMM::Monitor::Agents->instance();
	my $monitor	= MMM::Monitor::Monitor->instance();
	my $roles	= MMM::Monitor::Roles->instance();

	my $ret = '';
	if ($monitor->is_passive) {
		$ret .= "--- Monitor is in PASSIVE MODE ---\n";
		$ret .= sprintf("Cause: %s\n", $monitor->passive_info);
		$ret =~ s/^/# /mg;
	}
	$ret .= $agents->get_status_info(1);
	$ret .= $roles->get_preference_info();
	return $ret;
}

sub checks {
	my $host	= shift || 'all';
	my $check	= shift || 'all';

	my $checks	= MMM::Monitor::ChecksStatus->instance();
	my $ret = '';

	my $dateformat = Log::Log4perl::DateFormat->new('yyyy/MM/dd HH:mm:ss');

	my @valid_checks = qw(ping mysql rep_threads rep_backlog);
	return "ERROR: Unknown check '$check'!" unless ($check eq 'all' || grep(/^$check$/, @valid_checks));

	if ($host ne 'all') {
		return "ERROR: Unknown host name '$host'!" unless (defined($main::config->{host}->{$host}));
		if ($check ne 'all') {
			return sprintf("%s  %s  [last change: %s]  %s",
				$host,
				$check,
				$dateformat->format($checks->last_change($host, $check)),
				$checks->message($host, $check)
			);
		}
		foreach $check (@valid_checks) {
			$ret .= sprintf("%s  %-11s  [last change: %s]  %s\n",
				$host,
				$check,
				$dateformat->format($checks->last_change($host, $check)),
				$checks->message($host, $check)
			);
		}
		return $ret;
	}

	my $len = 0;
	foreach my $host (keys(%{$main::config->{host}})) { $len = max($len, length $host) }

	if ($check ne 'all') {
		foreach my $host (keys(%{$main::config->{host}})) {
			$ret .= sprintf("%*s  %s  [last change: %s]  %s\n",
				$len * -1,
				$host,
				$check,
				$dateformat->format($checks->last_change($host, $check)),
				$checks->message($host, $check)
			);
		}
		return $ret;
	}
	foreach my $host (keys(%{$main::config->{host}})) {
		foreach $check (@valid_checks) {
			$ret .= sprintf("%*s  %-11s  [last change: %s]  %s\n",
				$len * -1,
				$host,
				$check,
				$dateformat->format($checks->last_change($host, $check)),
				$checks->message($host, $check)
			);
		}
	}
	return $ret;
}

sub set_online($) {
	my $host	= shift;

	my $agents	= MMM::Monitor::Agents->instance();

	return "ERROR: Unknown host name '$host'!" unless (defined($main::config->{host}->{$host}));

	my $host_state = $agents->state($host);
	return "OK: This host is already ONLINE. Skipping command." if ($host_state eq 'ONLINE');

	unless ($host_state eq 'ADMIN_OFFLINE' || $host_state eq 'AWAITING_RECOVERY') {
		return "ERROR: Host '$host' is '$host_state' at the moment. It can't be switched to ONLINE.";
	}

	my $checks	= MMM::Monitor::ChecksStatus->instance();

	if ((!$checks->ping($host) || !$checks->mysql($host))) {
		return "ERROR: Checks ping and/or mysql are not ok for host '$host'. It can't be switched to ONLINE.";
	}

	# Check peer replication state
	if ($main::config->{host}->{$host}->{peer}) {
		my $peer = $main::config->{host}->{$host}->{peer};
		if ($agents->state($peer) eq 'ONLINE' && (!$checks->rep_threads($peer) || !$checks->rep_backlog($peer))) {
			return "ERROR: Some replication checks failed on peer '$peer'. We can't set '$host' online now. Please, wait some time.";
		}
	}

	my $agent = MMM::Monitor::Agents->instance()->get($host);
	if (!$agent->cmd_ping()) {
		return "ERROR: Can't reach agent daemon on '$host'! Can't switch its state!";
	}

	FATAL "Admin changed state of '$host' from $host_state to ONLINE";
	$agents->set_state($host, 'ONLINE');
	$agent->flapping(0);
	MMM::Monitor::Monitor->instance()->send_agent_status($host);

    return "OK: State of '$host' changed to ONLINE. Now you can wait some time and check its new roles!";
}

sub set_offline($) {
	my $host	= shift;

	my $agents	= MMM::Monitor::Agents->instance();

	return "ERROR: Unknown host name '$host'!" unless (defined($main::config->{host}->{$host}));

	my $host_state = $agents->state($host);
	return "OK: This host is already ADMIN_OFFLINE. Skipping command." if ($host_state eq 'ADMIN_OFFLINE');

	unless ($host_state eq 'ONLINE' || $host_state eq 'REPLICATION_FAIL' || $host_state eq 'REPLICATION_DELAY') {
		return "ERROR: Host '$host' is '$host_state' at the moment. It can't be switched to ADMIN_OFFLINE.";
	}

	my $agent = MMM::Monitor::Agents->instance()->get($host);
	return "ERROR: Can't reach agent daemon on '$host'! Can't switch its state!" unless ($agent->cmd_ping());

	FATAL "Admin changed state of '$host' from $host_state to ADMIN_OFFLINE";
	$agents->set_state($host, 'ADMIN_OFFLINE');
	MMM::Monitor::Roles->instance()->clear_roles($host);
	MMM::Monitor::Monitor->instance()->send_agent_status($host);

    return "OK: State of '$host' changed to ADMIN_OFFLINE. Now you can wait some time and check all roles!";
}

sub set_ip($$) {
	my $ip		= shift;
	my $host	= shift;

	return "ERROR: This command is only allowed in passive mode" unless (MMM::Monitor::Monitor->instance()->is_passive);

	my $agents	= MMM::Monitor::Agents->instance();
	my $roles	= MMM::Monitor::Roles->instance();

	my $role = $roles->find_by_ip($ip);

	return "ERROR: Unknown ip '$ip'!" unless (defined($role));
	return "ERROR: Unknown host name '$host'!" unless ($agents->exists($host));

	unless ($roles->can_handle($role, $host)) {
		return "ERROR: Host '$host' can't handle role '$role'. Following hosts could: " . join(', ', @{ $roles->get_valid_hosts($role) });
	}

	my $host_state = $agents->state($host);
	unless ($host_state eq 'ONLINE') {
		return "ERROR: Host '$host' is '$host_state' at the moment. Can't move role with ip '$ip' there.";
	}

	FATAL "Admin set role '$role($ip)' to host '$host'";

	$roles->set_role($role, $ip, $host);

	# Determine all roles and propagate them to agent objects.
	foreach my $one_host (@{ $roles->get_valid_hosts($role) }) {
		my $agent = $agents->get($one_host);
		my @agent_roles = sort($roles->get_host_roles($one_host));
		$agent->roles(\@agent_roles);
	}
	return "OK: Set role '$role($ip)' to host '$host'.";
}

sub move_role($$) {
	my $role	= shift;
	my $host	= shift;
	
	my $monitor	= MMM::Monitor::Monitor->instance();
	return "ERROR: This command is not allowed in passive mode" if ($monitor->is_passive);

	my $agents	= MMM::Monitor::Agents->instance();
	my $roles	= MMM::Monitor::Roles->instance();

	return "ERROR: Unknown role name '$role'!" unless ($roles->exists($role));
	return "ERROR: Unknown host name '$host'!" unless ($agents->exists($host));

	unless ($roles->is_exclusive($role)) {
		$roles->clear_balanced_role($host, $role);
		return "OK: Balanced role $role has been removed from host '$host'. Now you can wait some time and check new roles info!";
	}

	my $host_state = $agents->state($host);
	return "ERROR: Can't move role to host with state $host_state." unless ($host_state eq 'ONLINE');

	unless ($roles->can_handle($role, $host)) {
        return "ERROR: Host '$host' can't handle role '$role'. Only following hosts could: " . join(', ', @{ $roles->get_valid_hosts($role) });
	}
	
	my $old_owner = $roles->get_exclusive_role_owner($role);
	return "OK: Role is on '$host' already. Skipping command." if ($old_owner eq $host);

	my $agent = MMM::Monitor::Agents->instance()->get($host);
	return "ERROR: Can't reach agent daemon on '$host'! Can't move roles there!" unless ($agent->cmd_ping());

	if ($monitor->is_active && $roles->assigned_to_preferred_host($role)) {
		return "ERROR: Role '$role' is assigned to preferred host '$old_owner'. Can't move it!";
	}

	my $ip = $roles->get_exclusive_role_ip($role);
	return "Error: Role $role has no IP." unless ($ip);

	FATAL "Admin moved role '$role' from '$old_owner' to '$host'";

	# Assign role to new host
	$roles->set_role($role, $ip, $host);

	# Notify old host (if is_active_master_role($role) this will make the host non writable)
	$monitor->send_agent_status($old_owner);

	# Notify slaves (this will make them switch the master)
	$monitor->notify_slaves($host) if ($roles->is_active_master_role($role));

	# Notify new host (if is_active_master_role($role) this will make the host writable)
	$monitor->send_agent_status($host);
	
	return "OK: Role '$role' has been moved from '$old_owner' to '$host'. Now you can wait some time and check new roles info!";
	
}

sub forced_move_role($$) {
	my $role	= shift;
	my $host	= shift;
	
	my $monitor	= MMM::Monitor::Monitor->instance();
	return "ERROR: This command is not allowed in passive mode" if (MMM::Monitor::Monitor->instance()->is_passive);

	my $agents	= MMM::Monitor::Agents->instance();
	my $roles	= MMM::Monitor::Roles->instance();
	my $checks	= MMM::Monitor::ChecksStatus->instance();


	return "ERROR: Unknown role name '$role'!" unless ($roles->exists($role));
	return "ERROR: Unknown host name '$host'!" unless ($agents->exists($host));
	return "ERROR: move_role --forced may be used for the active master role only!" unless ($roles->is_active_master_role($role));

	my $host_state = $agents->state($host);
	unless ($host_state eq 'REPLICATION_FAIL' || $host_state eq 'REPLICATION_DELAY') {
		return "ERROR: Can't force move of role to host with state $host_state.";
	}

	unless ($roles->can_handle($role, $host)) {
        return "ERROR: Host '$host' can't handle role '$role'. Only following hosts could: " . join(', ', @{ $roles->get_valid_hosts($role) });
	}
	
	my $old_owner = $roles->get_exclusive_role_owner($role);
	return "OK: Role is on '$host' already. Skipping command." if ($old_owner eq $host);

	my $agent     = $agents->get($host);
	my $old_agent = $agents->get($old_owner);
	return "ERROR: Can't reach agent daemon on '$host'! Can't move roles there!" unless ($agent->cmd_ping());

	my $ip = $roles->get_exclusive_role_ip($role);
	return "Error: Role $role has no IP." unless ($ip);

	FATAL "Admin forced move of role '$role' from '$old_owner' to '$host'";

	# Assign role to new host
	$roles->set_role($role, $ip, $host);
	FATAL "State of host '$host' changed from $host_state to ONLINE (because of move_role --force)";
	$agent->state('ONLINE');

	if (!$checks->rep_threads($old_owner)) {
		FATAL "State of host '$old_owner' changed from ONLINE to REPLICATION_FAIL (because of move_role --force)";
		$old_agent->state('REPLICATION_FAIL');
		$roles->clear_roles($old_owner) if ($monitor->is_active);
	}
	elsif (!$checks->rep_backlog($old_owner)) {
		FATAL "State of host '$old_owner' changed from ONLINE to REPLICATION_DELAY (because of move_role --force)";
		$old_agent->state('REPLICATION_DELAY');
		$roles->clear_roles($old_owner) if ($monitor->is_active);
	}

	# Notify old host (this will make the host non writable)
	MMM::Monitor::Monitor->instance()->send_agent_status($old_owner);

	# Notify slaves (this will make them switch the master)
	MMM::Monitor::Monitor->instance()->notify_slaves($host);

	# Notify new host (this will make the host writable)
	MMM::Monitor::Monitor->instance()->send_agent_status($host);
	
	return "OK: Role '$role' has been moved from '$old_owner' to '$host' enforcedly. Now you can wait some time and check new roles info!";
	
}


=item mode

Get information about current mode (active, manual or passive)

=cut

sub mode() {
	my $monitor	= MMM::Monitor::Monitor->instance();
	return $monitor->get_mode_string();
}


=item set_active

Switch to active mode.

=cut

sub set_active() {
	my $monitor	= MMM::Monitor::Monitor->instance();

	return 'OK: Already in active mode.' if ($monitor->is_active);

	my $old_mode = $monitor->get_mode_string();
	INFO "Admin changed mode from '$old_mode' to 'ACTIVE'";
	
	if ($monitor->is_passive) {
		$monitor->set_active(); # so that we can send status to agents
		$monitor->cleanup_and_send_status();
		$monitor->passive_info('');
	}
	elsif ($monitor->is_manual) {
		# remove all roles from hosts which are not ONLINE
		my $roles	= MMM::Monitor::Roles->instance();
		my $agents	= MMM::Monitor::Agents->instance();
		my $checks	= MMM::Monitor::ChecksStatus->instance();
		foreach my $host (keys(%{$main::config->{host}})) {
			my $host_state = $agents->state($host);
			next if ($host_state eq 'ONLINE' || $roles->get_host_roles($host) == 0);
			my $agent = $agents->get($host);
			$roles->clear_roles($host);
			my $ret = $monitor->send_agent_status($host);
#			next if ($host_state eq 'REPLICATION_FAIL');
#			next if ($host_state eq 'REPLICATION_DELAY');
			# NOTE host_state should never be ADMIN_OFFLINE at this point
			if (!$ret) {
				ERROR sprintf("Can't send offline status notification to '%s' - killing it!", $host);
				$monitor->_kill_host($host, $checks->ping($host));
			}
		}
	}

	$monitor->set_active();
	return 'OK: Switched into active mode.';
}


=item set_manual

Switch to manual mode.

=cut

sub set_manual() {
	my $monitor	= MMM::Monitor::Monitor->instance();

	return 'OK: Already in manual mode.' if ($monitor->is_manual);

	my $old_mode = $monitor->get_mode_string();
	INFO "Admin changed mode from '$old_mode' to 'MANUAL'";

	if ($monitor->is_passive) {
		$monitor->set_manual(); # so that we can send status to agents
		$monitor->cleanup_and_send_status();
		$monitor->passive_info('');
	}

	$monitor->set_manual();
	return 'OK: Switched into manual mode.';
}


=item set_passive

Switch to passive mode.

=cut

sub set_passive() {
	my $monitor	= MMM::Monitor::Monitor->instance();

	return 'OK: Already in passive mode.' if ($monitor->is_passive);

	my $old_mode = $monitor->get_mode_string();
	INFO "Admin changed mode from '$old_mode' to 'PASSIVE'";

	$monitor->set_passive();
	$monitor->passive_info('Admin switched to passive mode.');
	return 'OK: Switched into passive mode.';
}

sub help() {
	return: "Valid commands are:
    help                              - show this message
    ping                              - ping monitor
    show                              - show status
    checks [<host>|all [<check>|all]] - show checks status
    set_online <host>                 - set host <host> online
    set_offline <host>                - set host <host> offline
    mode                              - print current mode.
    set_active                        - switch into active mode.
    set_manual                        - switch into manual mode.
    set_passive                       - switch into passive mode.
    move_role [--force] <role> <host> - move exclusive role <role> to host <host>
                                        (Only use --force if you know what you are doing!)
    set_ip <ip> <host>                - set role with ip <ip> to host <host>
";
}

1;
