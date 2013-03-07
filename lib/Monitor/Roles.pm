package MMM::Monitor::Roles;
use base 'Class::Singleton';

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use MMM::Monitor::Agents;
use MMM::Monitor::Role;

our $VERSION = '0.01';

=head1 NAME

MMM::Monitor::Roles - holds information for all roles

=cut

sub _new_instance($) {
	my $class = shift;

	my $self = {};

	# create list of roles - each role will be orphaned by default
	foreach my $role (keys(%{$main::config->{role}})) {
		my $role_info = $main::config->{role}->{$role};
		my $ips = {};
		foreach my $ip (@{$role_info->{ips}}) {
			$ips->{$ip} = { 'assigned_to'	=> '' }
		}
		$self->{$role} = {
			mode	=> $role_info->{mode},
			hosts	=> $role_info->{hosts},
			ips		=> $ips
		};
		if ($role_info->{mode} eq 'exclusive' && $role_info->{prefer}) {
			$self->{$role}->{prefer} = $role_info->{prefer};
		}
	}

	return bless $self, $class; 
}


=head1 FUNCTIONS

=over 4

=item assign($role, $host)

Assign role $role to host $host

=cut

sub assign($$$) {
	my $self	= shift;
	my $role	= shift;
	my $host	= shift;

	LOGDIE "Can't assign role '$role' - no host given" unless (defined($host));

	# Check if the ip is still configured for this role
	unless (defined($self->{$role->name}->{ips}->{$role->ip})) {
		WARN sprintf("Detected configuration change: ip '%s' was removed from role '%s'", $role->ip, $role->name);
		return;
	}
	INFO sprintf("Adding role '%s' with ip '%s' to host '%s'", $role->name, $role->ip, $host);

	$self->{$role->name}->{ips}->{$role->ip}->{assigned_to} = $host;
}

=item get_role_hosts($role)

Get all hosts which may handle role $role

=cut

sub get_role_hosts($$) {
	my $self	= shift;
	my $role	= shift;

	return () unless (defined($role));

	my $role_info = $self->{$role};
	return () unless $role_info;

	return @{$role_info->{hosts}};
}


=item get_host_roles($host)

Get all roles assigned to host $host

=cut

sub get_host_roles($$) {
	my $self	= shift;
	my $host	= shift;

	return () unless (defined($host));

	my @roles	= ();
	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};
		foreach my $ip (keys(%{$role_info->{ips}})) {
			my $ip_info = $role_info->{ips}->{$ip};
			next unless ($ip_info->{assigned_to} eq $host);
			push(@roles, new MMM::Monitor::Role::(name => $role, ip => $ip));
		}
	}
	return @roles;
}


=item host_has_roles($host)

Check whether there are roles assigned to host $host

=cut

sub host_has_roles($$) {
	my $self	= shift;
	my $host	= shift;

	return 0 unless (defined($host));

	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};
		foreach my $ip (keys(%{$role_info->{ips}})) {
			my $ip_info = $role_info->{ips}->{$ip};
			return 1 if ($ip_info->{assigned_to} eq $host);
		}
	}
	return 0;
}


=item count_host_roles($host)

Count all roles assigned to host $host

=cut

sub count_host_roles($$) {
	my $self	= shift;
	my $host	= shift;

	return 0 unless (defined($host));

	my $cnt	= 0;
	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};
		foreach my $ip (keys(%{$role_info->{ips}})) {
			my $ip_info = $role_info->{ips}->{$ip};
			next if ($ip_info->{assigned_to} ne $host);
			$cnt++;
			$cnt -= 0.5 if ($role eq $main::config->{active_master_role});
		}
	}
	return $cnt;
}


=item get_active_master

Get the host with the active master-role

=cut

sub get_active_master($) {
	my $self	= shift;

	my $role = $self->{$main::config->{active_master_role}};
	return '' unless $role;

	my @ips = keys( %{ $role->{ips} } );
	return $role->{ips}->{$ips[0]}->{assigned_to};
}


=item get_passive_master

Get the passive master

=cut

sub get_passive_master($) {
	my $self	= shift;

	my $role = $self->{$main::config->{active_master_role}};
	my $active_master = $self->get_active_master();
	return '' unless $role;
	return '' unless $active_master;

	foreach my $host ( @{ $role->{hosts} } ) {
		return $host if ($host ne $active_master);
	}
	return '';
}


=item get_first_master

Get the first master

=cut

sub get_first_master($) {
	my $self	= shift;

	my $role = $self->{$main::config->{active_master_role}};
	return '' unless $role;
	return '' unless $role->{hosts}[0];
	return $role->{hosts}[0];
}


=item get_second_master

Get the second master

=cut

sub get_second_master($) {
	my $self	= shift;

	my $role = $self->{$main::config->{active_master_role}};
	return '' unless $role;
	return '' unless $role->{hosts}[1];
	return $role->{hosts}[1];
}


=item get_master_hosts

Get the hosts which can handle the active master-role

=cut

sub get_master_hosts($) {
	my $self	= shift;

	my $role = $self->{$main::config->{active_master_role}};
	return '' unless $role;
	return $self->{$role}->{hosts};
}


=item get_exclusive_role_owner($role)

Get the host which has the exclusive role $role assigned

=cut

sub get_exclusive_role_owner($$) {
	my $self	= shift;
	my $role	= shift;

	my $role_info = $self->{$role};
	return '' unless $role_info;

	my @ips = keys( %{ $role_info->{ips} } );
	return $role_info->{ips}->{$ips[0]}->{assigned_to};
}


=item get_exclusive_role_ip($role)

Get the ip of an exclusive role $role

=cut

sub get_exclusive_role_ip($$) {
	my $self	= shift;
	my $role	= shift;

	my $role_info = $self->{$role};
	return undef unless $role_info;

	my @ips = keys( %{ $role_info->{ips} } );
	return $ips[0];
}


=item assigned_to_preferred_host($role)

Check if role is assigned to preferred host

=cut

sub assigned_to_preferred_host($$) {
	my $self	= shift;
	my $role	= shift;

	my $role_info = $self->{$role};
	return undef unless $role_info;
	return undef unless ($role_info->{prefer});

	my @ips = keys( %{ $role_info->{ips} } );
	return ($role_info->{ips}->{$ips[0]}->{assigned_to} eq $role_info->{prefer});
	
}


=item clear_roles($host)

Remove all roles from host $host.

=cut

sub clear_roles($$) {
	my $self	= shift;
	my $host	= shift;

	INFO "Removing all roles from host '$host':";

	my $orphaned_master_role = 0;
	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};
		foreach my $ip (keys(%{$role_info->{ips}})) {
			my $ip_info = $role_info->{ips}->{$ip};
			next unless ($ip_info->{assigned_to} eq $host);
			INFO "    Removed role '$role($ip)' from host '$host'";
			$ip_info->{assigned_to} = '';
			$orphaned_master_role = 1 if ($role eq $main::config->{active_master_role});
		}
	}
	return $orphaned_master_role;
}


=item clear_balanced_role($host, $role)

Remove balanced role $role from host $host.

=cut

sub clear_balanced_role($$$) {
	my $self	= shift;
	my $host	= shift;
	my $role	= shift;

	INFO "Removing balanced role $role from host '$host':";

	my $role_info = $self->{$role};
	return 0 unless $role_info;
	my $cnt = 0;
	next unless ($role_info->{mode} eq 'balanced');
	foreach my $ip (keys(%{$role_info->{ips}})) {
		my $ip_info = $role_info->{ips}->{$ip};
		next unless ($ip_info->{assigned_to} eq $host);
		$cnt++;
		INFO "    Removed role '$role($ip)' from host '$host'";
		$ip_info->{assigned_to} = '';
	}
	return $cnt;
}


=item find_eligible_host($role)

find host which can take over the role $role

=cut

sub find_eligible_host($$) {
	my $self	= shift;
	my $role	= shift;

	my $min_host	= '';
	my $min_count	= 0;

	my $agents = MMM::Monitor::Agents->instance();

	# Maybe role has a preferred hosts
	if ($self->{$role}->{prefer}) {
		my $host = $self->{$role}->{prefer};
 		if ($agents->{$host}->state eq 'ONLINE' && !$agents->{$host}->agent_down) {
			return $host;
		}
	}

	# Use host with fewest roles
	foreach my $host ( @{ $self->{$role}->{hosts} } ) {
		next unless ($agents->{$host}->state eq 'ONLINE');
		next if ($agents->{$host}->agent_down);
		my $cnt = $self->count_host_roles($host);
		next unless ($cnt < $min_count || $min_host eq '');
		$min_host	= $host;
		$min_count	= $cnt;
	}
	
	return $min_host;
}


=item find_eligible_hosts($role)

find all hosts which can take over the role $role.

=cut

sub find_eligible_hosts($$) {
	my $self	= shift;
	my $role	= shift;

	my $hosts	= {};

	my $agents = MMM::Monitor::Agents->instance();

	foreach my $host ( @{ $self->{$role}->{hosts} } ) {
		next unless ($agents->{$host}->state eq 'ONLINE');
		next if ($agents->{$host}->agent_down);
		my $cnt = $self->count_host_roles($host);
		$hosts->{$host} = $cnt;
	}
	
	return $hosts;
}


=item process_orphans

Find orphaned roles and assign them to a host if possible.

=cut

sub process_orphans($$) {
	my $self	= shift;
	my $mode	= shift;
	
	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};
		next if ($mode && $role_info->{mode} ne $mode);

		foreach my $ip (keys(%{$role_info->{ips}})) {
			my $ip_info = $role_info->{ips}->{$ip};
			next unless ($ip_info->{assigned_to} eq '');

			# Find host which can take over the role - skip if none found
			my $host = $self->find_eligible_host($role);
			last unless ($host);
			
			# Assign this ip to host
			$ip_info->{assigned_to} = $host;
			INFO "Orphaned role '$role($ip)' has been assigned to '$host'";
		}
	}
}


=item obey_preferences

Obey preferences by moving roles to preferred hosts

=cut
sub obey_preferences($) {
	my $self	= shift;

	my $agents	= MMM::Monitor::Agents->instance();

	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};

		next unless ($role_info->{prefer});

		my $host = $role_info->{prefer};

		next unless ($agents->{$host}->state eq 'ONLINE');
		next if ($agents->{$host}->agent_down);

		my @ips			= keys( %{ $role_info->{ips} } );
		my $ip			= $ips[0];
		my $ip_info		= $role_info->{ips}->{$ip};
		my $old_host	= $ip_info->{assigned_to};

		next if ($old_host eq $host);

		$ip_info->{assigned_to} = $host;
		INFO "Moving role '$role($ip)' from host '$old_host' to preferred host '$host'";
	}
}


=item get_preference_info

Get information about roles with preferred hosts

=cut
sub get_preference_info($) {
	my $self	= shift;

	my $ret = '';

	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};
		next unless ($role_info->{prefer});

		my $host       = $role_info->{prefer};
		my $other_host = $self->get_exclusive_role_owner($role);
		if ($host eq $other_host) {
			$ret .= "# Role $role is assigned to it's preferred host $host.\n";
		}
		elsif($other_host ne '') {
			$ret .= "# Role $role has $host configured as it's preferred host but is assigned to $other_host at the moment.\n";
		}
		else {
			$ret .= "# Role $role has $host configured as it's preferred host.\n";
		}
	}
	return $ret;
}


=item balance

Balance roles with mode 'balanced'

=cut

sub balance($) {
	my $self	= shift;
	
	foreach my $role (keys(%$self)) {
		my $role_info = $self->{$role};

		next unless ($role_info->{mode} eq 'balanced');

		my $hosts = $self->find_eligible_hosts($role);
		next if (scalar(keys(%$hosts)) < 2);

		while (1) {
			my $max_host = '';
			my $min_host = '';
			foreach my $host (keys(%$hosts)) {
				$max_host = $host if ($max_host eq '' || $hosts->{$host} > $hosts->{$max_host});
				$min_host = $host if ($min_host eq '' || $hosts->{$host} < $hosts->{$min_host});
			}
			
			if ($hosts->{$max_host} - $hosts->{$min_host} <= 1) {
				last;
			}
			
			$self->move_one_ip($role, $max_host, $min_host);
			$hosts->{$max_host}--;
			$hosts->{$min_host}++;
		}
	}
}


=item move_one_ip($role, $host1, $host2)

Move one IP of role $role from $host1 to $host2.

=cut

sub move_one_ip($$$$) {
	my $self	= shift;
	my $role	= shift;
	my $host1	= shift;
	my $host2	= shift;
	
	foreach my $ip (keys(%{$self->{$role}->{ips}})) {
		my $ip_info = $self->{$role}->{ips}->{$ip};
		next unless ($ip_info->{assigned_to} eq $host1);

		INFO "Moving role '$role($ip)' from host '$host1' to host '$host2'";
		$ip_info->{assigned_to} = $host2;
		return 1;
	}

	# No ip was moved
	return 0;
}


=item find_by_ip($ip)

Find name of role with IP $ip.

=cut

sub find_by_ip($$) {
	my $self	= shift;
	my $ip		= shift;

	foreach my $role (keys(%$self)) {
		return $role if (defined($self->{$role}->{ips}->{$ip}));
	}
	
	return undef;
}


=item set_role($role, $ip, $host)

Set role $role with IP $ip to host $host.

NOTE: No checks are done. Caller should assure that:
Role is valid, IP is valid, Host is valid, Host can handle role

=cut
sub set_role($$$$) {
	my $self	= shift;
	my $role	= shift;
	my $ip		= shift;
	my $host	= shift;

	$self->{$role}->{ips}->{$ip}->{assigned_to} = $host;
}


=item exists($role)

Check if role $role exists.

=cut
sub exists($$) {
	my $self	= shift;
	my $role	= shift;
	return defined($self->{$role});
}


=item exists_ip($role, $ip)

Check if role $role with IP $ip exists.

=cut

sub exists_ip($$$) {
	my $self	= shift;
	my $role	= shift;
	my $ip		= shift;
	return 0 unless defined($self->{$role});
	return defined($self->{$role}->{ips}->{$ip});
}


=item is_exclusive($role)

Determine whether given role is an exclusive role.

=cut

sub is_exclusive($$) {
	my $self	= shift;
	my $role	= shift;
	return 0 unless defined($self->{$role});
	return ($self->{$role}->{mode} eq 'exclusive');
}


=item get_valid_hosts($role)

Get all valid hosts for role $role.

=cut

sub get_valid_hosts($$) {
	my $self	= shift;
	my $role	= shift;
	return () unless defined($self->{$role});
	return $self->{$role}->{hosts};
}


=item can_handle($role, $host)

Check if host $host can handle role $role.

=cut

sub can_handle($$$) {
	my $self	= shift;
	my $role	= shift;
	my $host	= shift;
	return 0 unless defined($self->{$role});
	return grep({$_ eq $host} @{$self->{$role}->{hosts}});
}


=item is_master($host)

Check if host $host can handle role $role.

=cut

sub is_master($$) {
	my $self	= shift;
	my $host	= shift;
	my $role = $self->{$main::config->{active_master_role}};
	return 0 unless defined($role);
	return grep({$_ eq $host} @{$role->{hosts}});
}


=item is_active_master_role($role)

Check whether $role is the active master role.

=cut

sub is_active_master_role($$) {
	my $self	= shift;
	my $role	= shift;
	
	return ($role eq $main::config->{active_master_role});
}

1;
