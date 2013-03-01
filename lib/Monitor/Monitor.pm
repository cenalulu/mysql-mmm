package MMM::Monitor::Monitor;

use strict;
use warnings FATAL => 'all';
use threads;
use threads::shared;
use Algorithm::Diff;
use Data::Dumper;
use DBI;
use Errno qw(EINTR);
use Fcntl qw(F_SETFD F_GETFD FD_CLOEXEC);
use File::Temp;
use Log::Log4perl qw(:easy);
use Thread::Queue;
use MMM::Monitor::Agents;
use MMM::Monitor::Checker;
use MMM::Monitor::ChecksStatus;
use MMM::Monitor::Commands;
use MMM::Monitor::NetworkChecker;
use MMM::Monitor::Role;
use MMM::Monitor::Roles;
use MMM::Monitor::StartupStatus;

=head1 NAME

MMM::Monitor::Monitor - single instance class with monitor logic

=cut

our $VERSION = '0.01';

use constant MMM_MONITOR_MODE_PASSIVE => 0;
use constant MMM_MONITOR_MODE_ACTIVE  => 1;
use constant MMM_MONITOR_MODE_MANUAL  => 2;
use constant MMM_MONITOR_MODE_WAIT    => 3;

use Class::Struct;

sub instance() {
	return $main::monitor;
}

struct 'MMM::Monitor::Monitor' => {
	checker_queue		=> 'Thread::Queue',
	checks_status		=> 'MMM::Monitor::ChecksStatus',
	command_queue		=> 'Thread::Queue',
	result_queue		=> 'Thread::Queue',
	roles				=> 'MMM::Monitor::Roles',
	mode				=> '$',
	passive_info		=> '$',
	kill_host_bin		=> '$'
};



=head1 FUNCTIONS

=over 4

=item init

Init queues, single instance classes, ... and try to determine status.

=cut

sub init($) {
	my $self = shift;

	#___________________________________________________________________________
	#
	# Wait until network connection is available
	#___________________________________________________________________________

	INFO "Waiting for network connection...";
	unless (MMM::Monitor::NetworkChecker->wait_for_network()) {
		INFO "Received shutdown request while waiting for network connection.";
		return 0;
	}
	INFO "Network connection is available.";


	#___________________________________________________________________________
	#
	# Create thread queues and other stuff... 
	#___________________________________________________________________________

	my $agents = MMM::Monitor::Agents->instance();

	$self->checker_queue(new Thread::Queue::);
	$self->checks_status(MMM::Monitor::ChecksStatus->instance());
	$self->command_queue(new Thread::Queue::);
	$self->result_queue(new Thread::Queue::);
	$self->roles(MMM::Monitor::Roles->instance());
	$self->passive_info('');

	if ($main::config->{monitor}->{mode} eq 'active') {
		$self->mode(MMM_MONITOR_MODE_ACTIVE);
	}
	elsif ($main::config->{monitor}->{mode} eq 'manual') {
		$self->mode(MMM_MONITOR_MODE_MANUAL);
	}
	elsif ($main::config->{monitor}->{mode} eq 'wait') {
		$self->mode(MMM_MONITOR_MODE_WAIT);
	}
	elsif ($main::config->{monitor}->{mode} eq 'passive') {
		$self->mode(MMM_MONITOR_MODE_PASSIVE);
		$self->passive_info('Configured to start up in passive mode.');
	}
	else {
		LOGDIE "Something very, very strange just happend - dieing..."
	}


	#___________________________________________________________________________
	#
	# Check kill host binary
	#___________________________________________________________________________

	my $kill_host_bin = $main::config->{monitor}->{kill_host_bin};
	$kill_host_bin = $main::config->{monitor}->{bin_path} . "/monitor/$kill_host_bin" unless ($kill_host_bin =~ /^\/.*/);
	if (!-f $kill_host_bin) {
		WARN sprintf('No binary found for killing hosts (%s).', $kill_host_bin);
	}
	elsif (!-x _) {
		WARN sprintf('Binary for killing hosts (%s) is not executable.', $kill_host_bin);
	}
	else {
		$self->kill_host_bin($kill_host_bin);
	}


	my $checks	= $self->checks_status;

	
	#___________________________________________________________________________
	#
	# Check replication setup of master hosts
	#___________________________________________________________________________

	$self->check_master_configuration();


	#___________________________________________________________________________
	#
	# Fetch stored status, agent status and system status
	#___________________________________________________________________________

	$agents->load_status();	# load stored status


	my $startup_status	= new MMM::Monitor::StartupStatus; 

	my $res;

	foreach my $host (keys(%{$main::config->{host}})) {

		my $agent		= $agents->get($host);

		$startup_status->set_stored_status($host, $agent->state, $agent->roles);

		#_______________________________________________________________________
		#
		# Get AGENT status
		#_______________________________________________________________________

		$res = $agent->cmd_get_agent_status(2);

		if ($res =~ /^OK/) {
			my ($msg, $state, $roles_str, $master) = split('\|', $res);
			my @roles_str_arr = sort(split(/\,/, $roles_str));
			my @roles;

			foreach my $role_str (@roles_str_arr) {
				my $role = MMM::Monitor::Role->from_string($role_str);
				push(@roles, $role) if (defined($role));
			}

			$startup_status->set_agent_status($host, $state, \@roles, $master);
		}
		elsif ($agent->state ne 'ADMIN_OFFLINE') {
			if ($checks->ping($host) && $checks->mysql($host) && !$agent->agent_down()) {
				ERROR "Can't reach agent on host '$host'";
				$agent->agent_down(1);
			}
			ERROR "The status of the agent on host '$host' could not be determined (answer was: $res).";
		}
		

		#_______________________________________________________________________
		#
		# Get SYSTEM status
		#_______________________________________________________________________

		$res = $agent->cmd_get_system_status(2);

		if ($res =~ /^OK/) {
			my ($msg, $writable, $roles_str, $master_ip) = split('\|', $res);
			my @roles_str_arr = sort(split(/\,/, $roles_str));
			my @roles;

			foreach my $role_str (@roles_str_arr) {
				my $role = MMM::Monitor::Role->from_string($role_str);
				push(@roles, $role) if (defined($role));
			}

			my $master = '';
			if (defined($master_ip)) {
			    foreach my $a_host (keys(%{$main::config->{host}})) {
					$master = $a_host if ($main::config->{host}->{$a_host}->{ip} eq $master_ip);
				}
			}
			$startup_status->set_system_status($host, $writable, \@roles, $master);
		}
		elsif ($agent->state ne 'ADMIN_OFFLINE') {
			if ($checks->ping($host) && $checks->mysql($host) && !$agent->agent_down()) {
				ERROR "Can't reach agent on host '$host'";
				$agent->agent_down(1);
			}
			ERROR "The status of the system '$host' could not be determined (answer was: $res).";
		}
	}

	my $conflict = $startup_status->determine_status();

	DEBUG "STATE INFO\n", Data::Dumper->Dump([$startup_status], ['Startup status']);
	INFO $startup_status->to_string();

	foreach my $host (keys(%{$startup_status->{result}})) {
		my $agent = $agents->get($host);
		$agent->state($startup_status->{result}->{$host}->{state});
		foreach my $role (@{$startup_status->{result}->{$host}->{roles}}) {
			$self->roles->set_role($role->name, $role->ip, $host);
		}
	}

	if ($conflict && $main::config->{monitor}->{careful_startup}) {
		$self->set_passive();
		$self->passive_info("Conflicting roles during startup:\n\n" . $startup_status->to_string());
	}
	elsif (!$self->is_passive) {
		$self->cleanup_and_send_status();
	}
	
	INFO "Monitor started in active mode."  if ($self->mode == MMM_MONITOR_MODE_ACTIVE);
	INFO "Monitor started in manual mode."  if ($self->mode == MMM_MONITOR_MODE_MANUAL);
	INFO "Monitor started in wait mode."    if ($self->mode == MMM_MONITOR_MODE_WAIT);
	INFO "Monitor started in passive mode." if ($self->mode == MMM_MONITOR_MODE_PASSIVE);

	return 1;
}

sub check_master_configuration($) {
	my $self	= shift;

	# Get masters
	my @masters = $self->roles->get_role_hosts($main::config->{active_master_role});

	if (scalar(@masters) < 2) {
		WARN "Only one host configured which can handle the active master role. Skipping check of master-master configuration.";
		return;
	}
	if (scalar(@masters) > 2) {
		LOGDIE "There are more than two hosts configured which can handle the active master role.";
	}


	# Check status of masters
	my $checks	= $self->checks_status;
	foreach my $master (@masters) {
		next if ($checks->mysql($master));
		WARN "Check 'mysql' is in state 'failed' on host '$master'. Skipping check of master-master configuration.";
		return;
	}


	# Connect to masters
	my ($master1, $master2) = @masters;
	my $master1_info = $main::config->{host}->{$master1};
	my $master2_info = $main::config->{host}->{$master2};

	my $dsn1	= sprintf("DBI:mysql:host=%s;port=%s;mysql_connect_timeout=3", $master1_info->{ip}, $master1_info->{mysql_port});
	my $dsn2	= sprintf("DBI:mysql:host=%s;port=%s;mysql_connect_timeout=3", $master2_info->{ip}, $master2_info->{mysql_port});

	my $eintr	= EINTR;

	my $dbh1;
CONNECT1: {
	DEBUG "Connecting to master 1";
	$dbh1	= DBI->connect($dsn1, $master1_info->{monitor_user}, $master1_info->{monitor_password}, { PrintError => 0 });
	unless ($dbh1) {
		redo CONNECT1 if ($DBI::err == 2003 && $DBI::errstr =~ /\($eintr\)/);
		WARN "Couldn't connect to  '$master1'. Skipping check of master-master replication." . $DBI::err . " " . $DBI::errstr;
	}
}

	my $dbh2;
CONNECT2: {
	DEBUG "Connecting to master 2";
	$dbh2	= DBI->connect($dsn2, $master2_info->{monitor_user}, $master2_info->{monitor_password}, { PrintError => 0 });
	unless ($dbh2) {
		redo CONNECT2 if ($DBI::err == 2003 && $DBI::errstr =~ /\($eintr\)/);
		WARN "Couldn't connect to  '$master2'. Skipping check of master-master replication." . $DBI::err . " " . $DBI::errstr;
	}
}


	# Check replication peers
	my $slave_status1 = $dbh1->selectrow_hashref('SHOW SLAVE STATUS');
	my $slave_status2 = $dbh2->selectrow_hashref('SHOW SLAVE STATUS');

	WARN "$master1 is not replicating from $master2" if (!defined($slave_status1) || $slave_status1->{Master_Host} ne $master2_info->{ip});
	WARN "$master2 is not replicating from $master1" if (!defined($slave_status2) || $slave_status2->{Master_Host} ne $master1_info->{ip});


	# Check auto_increment_offset and auto_increment_increment
	my ($offset1, $increment1) = $dbh1->selectrow_array('select @@auto_increment_offset, @@auto_increment_increment');
	my ($offset2, $increment2) = $dbh2->selectrow_array('select @@auto_increment_offset, @@auto_increment_increment');

	unless (defined($offset1) && defined($increment1)) {
		WARN "Couldn't get value of auto_increment_offset/auto_increment_increment from host $master1. Skipping check of master-master replication.";
		return;
	}
	unless (defined($offset2) && defined($increment2)) {
		WARN "Couldn't get value of auto_increment_offset/auto_increment_increment from host $master2. Skipping check of master-master replication.";
		return;
	}
	
	WARN "auto_increment_increment should be identical on both masters ($master1: $increment1 , $master2: $increment2)" unless ($increment1 == $increment2);
	WARN "auto_increment_offset should be different on both masters ($master1: $offset1 , $master2: $offset2)" unless ($offset1 != $offset2);
	WARN "$master1: auto_increment_increment ($increment1) should be >= 2" unless ($increment1 >= 2);
	WARN "$master2: auto_increment_increment ($increment2) should be >= 2" unless ($increment2 >= 2);
	WARN "$master1: auto_increment_offset ($offset1) should not be greater than auto_increment_increment ($increment1)" unless ($offset1 <= $increment1);
	WARN "$master2: auto_increment_offset ($offset2) should not be greater than auto_increment_increment ($increment2)" unless ($offset2 <= $increment2);

}


=item main

Main thread

=cut

sub main($) {
	my $self	= shift;

	# Delay execution so we can reap all childs before spawning the checker threads.
	# This prevents a segfault if a SIGCHLD arrives during creation of a thread.
	# See perl bug #60724
	sleep(3);

	# Spawn checker threads
	my @checks	= keys(%{$main::config->{check}});
	my @threads;

	push(@threads, new threads(\&MMM::Monitor::NetworkChecker::main));
	push(@threads, new threads(\&MMM::Monitor::Commands::main, $self->result_queue, $self->command_queue));

	foreach my $check_name (@checks) {
		push(@threads, new threads(\&MMM::Monitor::Checker::main, $check_name, $self->checker_queue));
	}
	

	my $command_queue = $self->command_queue;

	while (!$main::shutdown) {
		$self->_process_check_results();
		$self->_check_host_states();
		$self->_process_commands();
		$self->_distribute_roles();
		$self->send_status_to_agents();

		# sleep 3 seconds, wake up if command queue gets filled
		lock($command_queue);
		cond_timedwait($command_queue, time() + 3); 
	}

	foreach my $thread (@threads) {
		$thread->join();
	}
}


=item _process_check_results

Process the results of the checker thread and change checks_status accordingly. Reads from check_queue.

=cut

sub _process_check_results($) {
	my $self = shift;

	my $cnt = 0;
	while (my $result = $self->checker_queue->dequeue_nb) {
		$cnt++ if $self->checks_status->handle_result($result);
	}
	return $cnt;
}


=item _check_host_states

Check states of hosts and change status/roles accordingly.

=cut

sub _check_host_states($) {
	my $self = shift;

	# Don't do anything if we have no network connection
	return if (!$main::have_net);

	my $checks	= $self->checks_status;
	my $agents	= MMM::Monitor::Agents->instance();

	my $active_master = $self->roles->get_active_master();

	foreach my $host (keys(%{$main::config->{host}})) {

		$agents->save_status() unless ($self->is_passive);

		my $agent		= $agents->get($host);
		my $state		= $agent->state;
		my $ping		= $checks->ping($host);
		my $mysql		= $checks->mysql($host);
		my $rep_backlog	= $checks->rep_backlog($host);
		my $rep_threads	= $checks->rep_threads($host);

		my $peer	= $main::config->{host}->{$host}->{peer};
		if (!$peer && $agent->mode eq 'slave') {
			$peer	= $active_master
		}

		my $peer_state = '';
		my $peer_online_since = 0;
		if ($peer) {
			$peer_state			= $agents->state($peer);
			$peer_online_since	= $agents->online_since($peer);
		}

		# Simply skip this host. It is offlined by admin
		next if ($state eq 'ADMIN_OFFLINE');

		########################################################################

		if ($state eq 'ONLINE') {

			# ONLINE -> HARD_OFFLINE
			unless ($ping && $mysql) {
				FATAL sprintf("State of host '%s' changed from %s to HARD_OFFLINE (ping: %s, mysql: %s)", $host, $state, ($ping? 'OK' : 'not OK'), ($mysql? 'OK' : 'not OK'));
				$agent->state('HARD_OFFLINE');
				next if ($self->is_manual);
				$self->roles->clear_roles($host);
				if (!$self->send_agent_status($host)) {
					ERROR sprintf("Can't send offline status notification to '%s' - killing it!", $host);
					$self->_kill_host($host, $checks->ping($host));
				}
				next;
			}

			# replication failure on active master is irrelevant.
			next if ($host eq $active_master);

			# ignore replication failure, if peer got online recently (60 seconds, default value of master-connect-retry)
			next if ($peer_state eq 'ONLINE' && $peer_online_since >= time() - 60);

			# ONLINE -> REPLICATION_FAIL
			if ($ping && $mysql && !$rep_threads && $peer_state eq 'ONLINE' && $checks->ping($peer) && $checks->mysql($peer)) {
				FATAL "State of host '$host' changed from $state to REPLICATION_FAIL";
				$agent->state('REPLICATION_FAIL');
				next if ($self->is_manual);
				$self->roles->clear_roles($host);
				if (!$self->send_agent_status($host)) {
					ERROR sprintf("Can't send offline status notification to '%s' - killing it!", $host);
					$self->_kill_host($host, $checks->ping($host));
				}
				next;
			}

			# ONLINE -> REPLICATION_DELAY
			if ($ping && $mysql && !$rep_backlog && $rep_threads && $peer_state eq 'ONLINE' && $checks->ping($peer) && $checks->mysql($peer)) {
				FATAL "State of host '$host' changed from $state to REPLICATION_DELAY";
				$agent->state('REPLICATION_DELAY');
				next if ($self->is_manual);
				$self->roles->clear_roles($host);
				if (!$self->send_agent_status($host)) {
					ERROR sprintf("Can't send offline status notification to '%s' - killing it!", $host);
					$self->_kill_host($host, $checks->ping($host));
				}
				next;
			}
			next;
		}

		########################################################################

		if ($state eq 'AWAITING_RECOVERY') {

			# AWAITING_RECOVERY -> HARD_OFFLINE
			unless ($ping && $mysql) {
				FATAL "State of host '$host' changed from $state to HARD_OFFLINE";
				$agent->state('HARD_OFFLINE');
				next;
			}

			# AWAITING_RECOVERY -> ONLINE (if host was offline for a short period)
			if ($ping && $mysql && $rep_backlog && $rep_threads) {
				my $state_diff  = time() - $agent->last_state_change;

				if ($agent->flapping) {
					my $check_state_diff  = time() - $checks->last_change($host);
					# set flapping host ONLINE because of auto_set_online
					next unless (defined($main::config->{monitor}->{auto_set_online}) && $main::config->{monitor}->{auto_set_online} > 0);
					next if ($check_state_diff < $main::config->{monitor}->{flap_duration});
					FATAL sprintf(
						"State of flapping host '%s' changed from %s to ONLINE because of auto_set_online and flap_duration(%d) seconds passed without another failure. It was in state AWAITING_RECOVERY for %d seconds",
						$host,
						$state,
						$main::config->{monitor}->{flap_duration},
						$state_diff
					);
					$agent->state('ONLINE');
					$self->send_agent_status($host);
					next;
				}

				my $uptime_diff = $agent->uptime - $agent->last_uptime;

				# set ONLINE because of small downtime
				if ($agent->last_uptime > 0 && $uptime_diff > 0 && $uptime_diff < 60) {
					FATAL sprintf("State of host '%s' changed from %s to ONLINE because it was down for only %d seconds", $host, $state, $uptime_diff);
					$agent->state('ONLINE');
					$self->send_agent_status($host);
					next;
				}
				# set ONLINE because of auto_set_online
				if (defined($main::config->{monitor}->{auto_set_online}) && $main::config->{monitor}->{auto_set_online} > 0 && $main::config->{monitor}->{auto_set_online} <= $state_diff) {
					FATAL sprintf("State of host '%s' changed from %s to ONLINE because of auto_set_online(%d seconds). It was in state AWAITING_RECOVERY for %d seconds", $host, $state, $main::config->{monitor}->{auto_set_online}, $state_diff);
					$agent->state('ONLINE');
					$self->send_agent_status($host);
					next;
				}
			}
			next;
		}

		########################################################################

		if ($state eq 'HARD_OFFLINE') {

			if ($ping && $mysql) {

				# only if we have an active master or the host can't be the active master 
				if ($active_master ne '' || !$self->roles->can_handle($main::config->{active_master_role}, $host)) {

					# HARD_OFFLINE -> REPLICATION_FAIL
					if (!$rep_threads) {
						FATAL "State of host '$host' changed from $state to REPLICATION_FAIL";
						$agent->state('REPLICATION_FAIL');
						$self->send_agent_status($host);
						next;
					}
	
					# HARD_OFFLINE -> REPLICATION_DELAY
					if (!$rep_backlog) {
						FATAL "State of host '$host' changed from $state to REPLICATION_DELAY";
						$agent->state('REPLICATION_DELAY');
						$self->send_agent_status($host);
						next;
					}
				}

				# HARD_OFFLINE -> AWAITING_RECOVERY
				FATAL "State of host '$host' changed from $state to AWAITING_RECOVERY";
				$agent->state('AWAITING_RECOVERY');
				$self->send_agent_status($host);
				next;
			}
		}

		########################################################################

		if ($state eq 'REPLICATION_FAIL') {
			# REPLICATION_FAIL -> REPLICATION_DELAY
			if ($ping && $mysql && !$rep_backlog && $rep_threads) {
				FATAL "State of host '$host' changed from $state to REPLICATION_DELAY";
				$agent->state('REPLICATION_DELAY');
				next;
			}
		}
		if ($state eq 'REPLICATION_DELAY') {
			# REPLICATION_DELAY -> REPLICATION_FAIL
			if ($ping && $mysql && !$rep_threads) {
				FATAL "State of host '$host' changed from $state to REPLICATION_FAIL";
				$agent->state('REPLICATION_FAIL');
				next;
			}
		}

		########################################################################

		if ($state eq 'REPLICATION_DELAY' || $state eq 'REPLICATION_FAIL') {
			if ($ping && $mysql && (($rep_backlog && $rep_threads) || $peer_state ne 'ONLINE')) {

				# REPLICATION_DELAY || REPLICATION_FAIL -> AWAITING_RECOVERY
				if ($agent->flapping) {
					FATAL "State of host '$host' changed from $state to AWAITING_RECOVERY (because it's flapping)";
					$agent->state('AWAITING_RECOVERY');
					$self->send_agent_status($host);
					next;
				}

				# REPLICATION_DELAY || REPLICATION_FAIL -> ONLINE
				FATAL "State of host '$host' changed from $state to ONLINE";
				$agent->state('ONLINE');
				$self->send_agent_status($host);
				next;
			}

			# REPLICATION_DELAY || REPLICATION_FAIL -> HARD_OFFLINE
			unless ($ping && $mysql) {
				FATAL sprintf("State of host '%s' changed from %s to HARD_OFFLINE (ping: %s, mysql: %s)", $host, $state, ($ping? 'OK' : 'not OK'), ($mysql? 'OK' : 'not OK'));
				$agent->state('HARD_OFFLINE');
				if (!$self->send_agent_status($host)) {
					ERROR sprintf("Can't send offline status notification to '%s' - killing it!", $host);
					$self->_kill_host($host, $checks->ping($host));
				}
				next;
			}
			next;
		}
	}

	if ($self->mode == MMM_MONITOR_MODE_WAIT) {
		my $master_one	= $self->roles->get_first_master();
		my $master_two	= $self->roles->get_second_master();
		my $state_one	= $agents->state($master_one);
		my $state_two	= $agents->state($master_two);

		if ($state_one eq 'ONLINE' && $state_two eq 'ONLINE') {
			INFO "Nodes $master_one and $master_two are ONLINE, switching from mode 'WAIT' to 'ACTIVE'.";
			$self->set_active();
		}
		elsif ($main::config->{monitor}->{wait_for_other_master} > 0 && ($state_one eq 'ONLINE' || $state_two eq 'ONLINE')) {
			my $living_master = $state_one eq 'ONLINE' ? $master_one : $master_two;
			my $dead_master   = $state_one eq 'ONLINE' ? $master_two : $master_one;

			if ($main::config->{monitor}->{wait_for_other_master} <= time() - $agents->online_since($living_master)) {
				$self->set_active();
				WARN sprintf("Master $dead_master did not come online for %d(wait_for_other_master) seconds. Switching from mode 'WAIT' to 'ACTIVE'", $main::config->{monitor}->{wait_for_other_master});
			}

		}
		if ($self->is_active) {
			# cleanup
			foreach my $host (keys(%{$main::config->{host}})) {
				my $host_state = $agents->state($host);
				next if ($host_state eq 'ONLINE' || $self->roles->get_host_roles($host) == 0); 
				my $agent = $agents->get($host); 
				$self->roles->clear_roles($host); 
				my $ret = $self->send_agent_status($host); 
#   			next if ($host_state eq 'REPLICATION_FAIL'); 
#   			next if ($host_state eq 'REPLICATION_DELAY'); 
				# NOTE host_state should never be ADMIN_OFFLINE at this point 
				if (!$ret) { 
					ERROR sprintf("Can't send offline status notification to '%s' - killing it!", $host); 
					$self->_kill_host($host, $checks->ping($host)); 
				} 
			}
		}
	}

	$agents->save_status() unless ($self->is_passive);
}


=item _distribute_roles

Distribute roles among the hosts.

=cut

sub _distribute_roles($) {
	my $self = shift;

	# Never change roles if we are in PASSIVE mode
	return if ($self->is_passive);

	my $old_active_master = $self->roles->get_active_master();
	
	# Process orphaned roles
	$self->roles->process_orphans('exclusive');
	$self->roles->process_orphans('balanced');

	# obey preferences
	$self->roles->obey_preferences() if ($self->is_active);

	# Balance roles
	$self->roles->balance();

	my $new_active_master = $self->roles->get_active_master();

	# notify slaves first, if master host has changed
	unless ($new_active_master eq $old_active_master) {
		$self->send_agent_status($old_active_master, $new_active_master) if ($old_active_master);
		$self->notify_slaves($new_active_master);
	}
}


=item cleanup_and_send_status()

Send status information to all agents and clean up old roles.

=cut
sub cleanup_and_send_status($) {
	my $self	= shift;

	my $agents = MMM::Monitor::Agents->instance();
	my $roles = MMM::Monitor::Roles->instance();

	my $active_master  = $roles->get_active_master();
	my $passive_master = $roles->get_passive_master();

	# Notify passive master first
	if ($passive_master ne '') {
		my $host = $passive_master;
		$self->send_agent_status($host);
		my $agent = $agents->get($host);
		$agent->cmd_clear_bad_roles(); # TODO check result
	}

	# Notify all slave hosts
	foreach my $host (keys(%{$main::config->{host}})) {
		next if ($self->roles->is_master($host));
		$self->send_agent_status($host);
		my $agent = $agents->get($host);
		$agent->cmd_clear_bad_roles(); # TODO check result
	}

	# Notify active master at the end
	if ($active_master ne '') {
		my $host = $active_master;
		$self->send_agent_status($host);
		my $agent = $agents->get($host);
		$agent->cmd_clear_bad_roles(); # TODO check result
	}
}


=item send_status_to_agents

Send status information to all agents.

=cut

sub send_status_to_agents($) {
	my $self	= shift;

	# Send status to all hosts
	my $master	= $self->roles->get_active_master();
	foreach my $host (keys(%{$main::config->{host}})) {
		$self->send_agent_status($host, $master);
	}
}


=item notify_slaves

Notify all slave hosts (used when master changes).

=cut

sub notify_slaves($$) {
	my $self		= shift;
	my $new_master	= shift;

	# Send status to all hosts with mode = 'slave'
	foreach my $host (keys(%{$main::config->{host}})) {
		next unless ($main::config->{host}->{$host}->{mode} eq 'slave');
		$self->send_agent_status($host, $new_master);
	}
}


=item send_agent_status($host[, $master])

Send status information to agent on host $host.

=cut

sub send_agent_status($$$) {
	my $self	= shift;
	my $host	= shift;
	my $master	= shift;

	# Never send anything to agents if we are in PASSIVE mode
	# Never send anything to agents if we have no network connection
	return if ($self->is_passive || !$main::have_net);

	# Determine active master if it was not passed
	$master = $self->roles->get_active_master() unless (defined($master));

	my $agent = MMM::Monitor::Agents->instance()->get($host);

	# Determine and set roles
	my @roles = sort($self->roles->get_host_roles($host));
	$agent->roles(\@roles);

	# Finally send command
	my $ret = $agent->cmd_set_status($master);

	unless ($ret) {
		# If ping is down, nothing will be send to agent. So this doesn't indicate that the agent is down.
		my $checks	= $self->checks_status;
		if ($checks->ping($host) && !$agent->agent_down()) {
			FATAL "Can't reach agent on host '$host'";
			$agent->agent_down(1);
		}
	}
	elsif ($agent->agent_down) {
		FATAL "Agent on host '$host' is reachable again";
		$agent->agent_down(0);
	}
	return $ret;
}


=item _kill_host

Process commands received from the command thread.

=cut

sub _kill_host($$$) {
	my $self		= shift;
	my $host		= shift;
	my $ping		= shift;

	if (!defined($self->kill_host_bin)) {
		FATAL sprintf("Could not kill host '%s' - there may be some duplicate ips now! (There's no binary configured for killing hosts.)", $host);
		return;
	}

	# Executing the kill_host_bin and capturing its output _and_ return code is a bit complicated.
	# We can't use backticks - $? (also called $CHILD_ERROR) will always be undefined because 
	# mmm_mond installs a custom signal handler for SIGCHLD.
	# So we use "system" instead of backticks and redirect the output to a temporary file.
	# To prevent race conditions we use tempfile instead of tmpname, clear the close-on-exec flag
	# and redirect the output of system to '/dev/fd/' . fileno(fh).
	my $fh = File::Temp::tempfile();
	my $flags = fcntl($fh, F_GETFD, 0);
	$flags &= ~FD_CLOEXEC;
	fcntl($fh, F_SETFD, $flags);

	my $command	= sprintf("%s %s %s", $self->kill_host_bin, $host, $ping);
	INFO sprintf("Killing host using command '%s'", $command);
	my $ret = system($command . sprintf(' >/dev/fd/%s 2>&1', fileno($fh)));
	# signal information in the lower 8 bits, exit code above that
	$ret = $ret >> 8;

	my $output = '';
	seek($fh, 0, 0);
	local $/;
	$output = <$fh>;
	close $fh;

	if ($ret == 0) {
		INFO sprintf("Output of kill host command was: %s", $output) if ($output ne "");
		return;
	}

	FATAL sprintf("Could not kill host '%s' - there may be some duplicate ips now! kill_host binary exited with '%s'. Output was: %s ", $host, $ret, $output);
	return;
}

=item _process_commands

Process commands received from the command thread.

=cut

sub _process_commands($) {
	my $self		= shift;

	# Handle all queued commands
	while (my $cmdline = $self->command_queue->dequeue_nb) {

		# Parse command
		my @args	= split(/\s+/, $cmdline);
		my $command	= shift @args;
		my $arg_cnt	= scalar(@args);
		my $res;

		# Execute command
		if    ($command eq 'help'			&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::help();							}
		elsif ($command eq 'ping'			&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::ping();							}
		elsif ($command eq 'show'			&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::show();							}
		elsif ($command eq 'checks'			&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::checks();						}
		elsif ($command eq 'checks'			&& $arg_cnt == 1) { $res = MMM::Monitor::Commands::checks($args[0]);				}
		elsif ($command eq 'checks'			&& $arg_cnt == 2) { $res = MMM::Monitor::Commands::checks($args[0], $args[1]);		}
		elsif ($command eq 'mode'			&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::mode();							}
		elsif ($command eq 'set_active'		&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::set_active();					}
		elsif ($command eq 'set_passive'	&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::set_passive();					}
		elsif ($command eq 'set_manual'		&& $arg_cnt == 0) { $res = MMM::Monitor::Commands::set_manual();					}
		elsif ($command eq 'set_online'		&& $arg_cnt == 1) { $res = MMM::Monitor::Commands::set_online ($args[0]);			}
		elsif ($command eq 'set_offline'	&& $arg_cnt == 1) { $res = MMM::Monitor::Commands::set_offline($args[0]);			}
		elsif ($command eq 'move_role'		&& $arg_cnt == 2) { $res = MMM::Monitor::Commands::move_role($args[0], $args[1]);	}
		elsif ($command eq 'move_role'		&& $arg_cnt == 3 && $args[0] eq "--force") {
			$res = MMM::Monitor::Commands::forced_move_role($args[1], $args[2]);
		}
		elsif ($command eq 'set_ip'			&& $arg_cnt == 2) { $res = MMM::Monitor::Commands::set_ip($args[0], $args[1]);		}
		else { $res = "Invalid command '$cmdline'\n\n" . MMM::Monitor::Commands::help(); }

		# Enqueue result
		$self->result_queue->enqueue($res);
	}
}


=item is_active()

Check if monitor is in active mode

=cut

sub is_active($$) {
	my $self	= shift;
	return ($self->mode == MMM_MONITOR_MODE_ACTIVE);
}


=item is_manual()

Check if monitor is in manual mode

=cut

sub is_manual($$) {
	my $self	= shift;
	return ($self->mode == MMM_MONITOR_MODE_MANUAL || $self->mode == MMM_MONITOR_MODE_WAIT);
}


=item is_passive()

Check if monitor is in passive mode

=cut

sub is_passive($$) {
	my $self	= shift;
	return ($self->mode == MMM_MONITOR_MODE_PASSIVE);
}


=item set_active()

Set mode to active

=cut

sub set_active($$) {
	my $self	= shift;
	$self->mode(MMM_MONITOR_MODE_ACTIVE);
}


=item set_manual()

Set mode to manual

=cut

sub set_manual($$) {
	my $self	= shift;
	$self->mode(MMM_MONITOR_MODE_MANUAL);
}


=item set_passive()

Set mode to passive

=cut

sub set_passive($$) {
	my $self	= shift;
	$self->mode(MMM_MONITOR_MODE_PASSIVE);
}


=item get_mode_string()

Get string representation of current mode

=cut

sub get_mode_string($) {
	my $self	= shift;
	return 'ACTIVE'  if ($self->mode == MMM_MONITOR_MODE_ACTIVE);
	return 'MANUAL'  if ($self->mode == MMM_MONITOR_MODE_MANUAL);
	return 'WAIT'    if ($self->mode == MMM_MONITOR_MODE_WAIT);
	return 'PASSIVE' if ($self->mode == MMM_MONITOR_MODE_PASSIVE);
	return 'UNKNOWN'; # should never happen
}

1;

