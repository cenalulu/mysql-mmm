package MMM::Monitor::StartupStatus;

use strict;
use warnings FATAL => 'all';
use List::Util qw(max);
use Log::Log4perl qw(:easy);
use MMM::Common::Role;
use MMM::Monitor::Role;
use MMM::Monitor::Roles;

our $VERSION = '0.01';

=head1 NAME

MMM::Monitor::StartupStatus - holds information about agent/system/stored status during startup

=cut

sub new($) {
	my $class   = shift;

	my $self = {
		roles => {},
		hosts => {},
		result=> {}
	};
	return bless $self, $class;
}


=head1 FUNCTIONS

=over 4

=item set_agent_status($host, $state, $roles, $master)

Set agent status

=cut

sub set_agent_status($$\@$) {
	my $self	= shift;
	my $host	= shift;
	my $state	= shift;
	my $roles	= shift;
	my $master	= shift;

	$self->{hosts}->{$host}				= {} unless (defined($self->{hosts}->{$host}));
	$self->{hosts}->{$host}->{agent}	= {
		state	=> $state,
		master	=> $master
	};
	foreach my $role (@{$roles}) {
		unless (MMM::Monitor::Roles->instance()->exists_ip($role->name, $role->ip)) {
			WARN "Detected change in role definitions: Role '$role' was removed.";
			next;
		}
		unless (MMM::Monitor::Roles->instance()->can_handle($role->name, $host)) {
			WARN "Detected change in role definitions: Host '$host' can't handle role '$role' anymore.";
			next;
		}
		my $role_str = $role->to_string();
		$self->{roles}->{$role_str}						= {} unless (defined($self->{roles}->{$role_str}));
		$self->{roles}->{$role_str}->{$host}			= {} unless (defined($self->{roles}->{$role_str}->{$host}));
		$self->{roles}->{$role_str}->{$host}->{agent}	= 1;
	}
}


=item set_stored_status($host, $state, $roles)

Set stored status

=cut

sub set_stored_status($$\@$) {
	my $self	= shift;
	my $host	= shift;
	my $state	= shift;
	my $roles	= shift;

	$self->{hosts}->{$host}				= {} unless (defined($self->{hosts}->{$host}));
	$self->{hosts}->{$host}->{stored}	= {
		state	=> $state,
	};
	foreach my $role (@{$roles}) {
		unless (MMM::Monitor::Roles->instance()->exists_ip($role->name, $role->ip)) {
			WARN "Detected change in role definitions: Role '$role' was removed.";
			next;
		}
		unless (MMM::Monitor::Roles->instance()->can_handle($role->name, $host)) {
			WARN "Detected change in role definitions: Host '$host' can't handle role '$role' anymore.";
			next;
		}
		my $role_str = $role->to_string();
		$self->{roles}->{$role_str}						= {} unless (defined($self->{roles}->{$role_str}));
		$self->{roles}->{$role_str}->{$host}			= {} unless (defined($self->{roles}->{$role_str}->{$host}));
		$self->{roles}->{$role_str}->{$host}->{stored}	= 1;
	}
}


=item set_system_status($host, $writable, $roles, $master)

Set system status

=cut

sub set_system_status($$\@$) {
	my $self	= shift;
	my $host	= shift;
	my $writable= shift;
	my $roles	= shift;
	my $master	= shift;

	$self->{hosts}->{$host}				= {} unless (defined($self->{hosts}->{$host}));
	$self->{hosts}->{$host}->{system}	= {
		writable=> $writable,
		master	=> $master
	};
	foreach my $role (@{$roles}) {
		unless (MMM::Monitor::Roles->instance()->exists_ip($role->name, $role->ip)) {
			WARN "Detected change in role definitions: Role '$role' was removed.";
			next;
		}
		unless (MMM::Monitor::Roles->instance()->can_handle($role->name, $host)) {
			WARN "Detected change in role definitions: Host '$host' can't handle role '$role' anymore.";
			next;
		}
		my $role_str = $role->to_string();
		$self->{roles}->{$role_str}						= {} unless (defined($self->{roles}->{$role_str}));
		$self->{roles}->{$role_str}->{$host}			= {} unless (defined($self->{roles}->{$role_str}->{$host}));
		$self->{roles}->{$role_str}->{$host}->{system}	= 1;
	}
}

sub determine_status() {
	my $self	= shift;
	my $roles	= MMM::Monitor::Roles->instance();

	my $is_manual = MMM::Monitor::Monitor->instance()->is_manual();

	my $conflict = 0;

    foreach my $host (keys(%{$main::config->{host}})) {

		# Figure out host state

		my $stored_state = 'UNKNOWN';
		my $agent_state  = 'UNKNOWN';
		my $state;

		$stored_state = $self->{hosts}->{$host}->{stored}->{state} if (defined($self->{hosts}->{$host}->{stored}->{state}));
		$agent_state  = $self->{hosts}->{$host}->{agent}->{state}  if (defined($self->{hosts}->{$host}->{agent}->{state} ));

		if (   $stored_state eq 'ADMIN_OFFLINE'     || $agent_state eq 'ADMIN_OFFLINE'    ) { $state = 'ADMIN_OFFLINE';     }
		elsif ($stored_state eq 'HARD_OFFLINE'      || $agent_state eq 'HARD_OFFLINE'     ) { $state = 'HARD_OFFLINE';      }
		elsif ($stored_state eq 'REPLICATION_FAIL'  || $agent_state eq 'REPLICATION_FAIL' ) { $state = 'REPLICATION_FAIL';  }
		elsif ($stored_state eq 'REPLICATION_DELAY' || $agent_state eq 'REPLICATION_DELAY') { $state = 'REPLICATION_DELAY'; }
		elsif ($stored_state eq 'ONLINE'            || $agent_state eq 'ONLINE'           ) { $state = 'ONLINE';            }
		else                                                                                { $state = 'AWAITING_RECOVERY'; }

		$self->{result}->{$host} = { state => $state, roles => [] };
	}

    foreach my $role_str (keys(%{$self->{roles}})) {
		my $role = MMM::Monitor::Role->from_string($role_str);
		next unless(defined($role));

		if ($roles->is_active_master_role($role->name)) {
			# active master role
			my $max        = 0;
			my $target     = undef;
			my $system_cnt = 0;
			foreach my $host (keys(%{$self->{roles}->{$role_str}})) {
				my $votes = 0;
				my $info  = $self->{roles}->{$role_str}->{$host};
				my $host_info = $self->{hosts}->{$host};

				# host is writable
				$votes += 4 if (defined($host_info->{system}->{writable}) && $host_info->{system}->{writable});

				# IP is configured
				if (defined($info->{system})) {
					$votes += 2;
					$system_cnt++;
				}

				$votes += 1 if (defined($info->{stored}));
				$votes += 1 if (defined($info->{agent}));

				foreach my $slave_host (keys(%{$self->{hosts}})) {
					my $slave_info = $self->{hosts}->{$slave_host};
					next if MMM::Monitor::Roles->instance()->is_master($slave_host);
					$votes++ if (defined($slave_info->{system}->{master}) && $slave_info->{system}->{master} eq $host);
				}
	
	
				my $state = $self->{result}->{$host}->{state};
				$votes = 0 if ($state eq 'ADMIN_OFFLINE');
				$votes = 0 if ($state eq 'HARD_OFFLINE' && !$is_manual);

				if ($votes > $max) {
					$target = $host;
					$max = $votes;
				}
			}
			if ($system_cnt > 1) {
				WARN "Role '$role_str' was configured on $system_cnt hosts during monitor startup.";
				$conflict = 1;
			}
			if (defined($target)) { 
				push (@{$self->{result}->{$target}->{roles}}, $role);
				my $state = $self->{result}->{$target}->{state};
				$self->{result}->{$target}->{state} = 'ONLINE' if (!$is_manual || $state eq 'REPLICATION_FAIL' || $state eq 'REPLICATION_DELAY');
			}
			next;
		}

		# Handle non-writer roles
		my $max        = 0;
		my $target     = undef;
		my $system_cnt = 0;
		foreach my $host (keys(%{$self->{roles}->{$role_str}})) {
			my $votes = 0;
			my $info  = $self->{roles}->{$role_str}->{$host};

			# IP is configured
			if (defined($info->{system})) {
				$votes += 4;
				$system_cnt++;
			}

			$votes += 2 if (defined($info->{stored}));
			$votes += 1 if (defined($info->{agent}));


			my $state = $self->{result}->{$host}->{state};
			if ($state eq 'ADMIN_OFFLINE' || (!$is_manual && $state ne 'ONLINE' && $state ne 'AWAITING_RECOVERY')) {
				$votes = 0;
			}
			if ($votes > $max) {
				$target    = $host;
				$max       = $votes;
			}
		}
		if ($system_cnt > 1) {
			WARN "Role '$role_str' was configured on $system_cnt hosts during monitor startup.";
		}
		if (defined($target)) { 
			push (@{$self->{result}->{$target}->{roles}}, $role);
			$self->{result}->{$target}->{state} = 'ONLINE' if ($self->{result}->{$target}->{state} eq 'AWAITING_RECOVERY');
		}
	}
	return $conflict;
}


sub to_string($) {
	my $self	= shift;
	my $ret = "Startup status:\n";
	$ret .= "\nRoles:\n";

    my $role_len = 4; # "Role"
    my $host_len = 6; # "Master"

    foreach my $role (keys(%{$main::config->{role}})) { $role_len = max($role_len, length $role) }
    foreach my $host (keys(%{$main::config->{host}})) { $host_len = max($host_len, length $host) }
	$role_len += 17; # "(999.999.999.999)"

	$ret .= sprintf("    %-*s  %-*s  %-6s  %-6s  %-5s\n", $role_len, 'Role', $host_len, 'Host', 'Stored', 'System', 'Agent');
	foreach my $role (keys(%{$self->{roles}})) {
		foreach my $host (keys(%{$self->{roles}->{$role}})) {
			my $info = $self->{roles}->{$role}->{$host};
			$ret .= sprintf("    %-*s  %-*s  %-6s  %-6s  %-5s\n", $role_len, $role, $host_len, $host,
				defined($info->{stored}) ? 'Yes'  : '-',
				defined($info->{system}) ? 'Yes'  : '-',
				defined($info->{agent})  ? 'Yes'  : '-'
			);
		}
	}

	$ret .= "\nHosts:\n";
	$ret .= sprintf("    %-*s  %-*s  %-8s  %-16s  %-16s\n", $host_len, 'Host', $host_len, 'Master', 'Writable', 'Stored state', 'Agent state');
	foreach my $host (keys(%{$self->{hosts}})) {
		my $info = $self->{hosts}->{$host};
		my $is_master = MMM::Monitor::Roles->instance()->is_master($host);
		$ret .= sprintf("    %-*s  %-*s  %-8s  %-16s  %-16s\n", $host_len, $host, $host_len,
			$is_master ? '-' : (defined($info->{system}->{master})   ? $info->{system}->{master}   : '?'),
			defined($info->{system}->{writable}) ? ($info->{system}->{writable} ? 'Yes' : 'No') : '?',
			defined($info->{stored}->{state})    ? $info->{stored}->{state} : '?',
			defined($info->{agent}->{state})     ? $info->{agent}->{state}  : '?',
		);
	}
	return $ret;
}

1;
