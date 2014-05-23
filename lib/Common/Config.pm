package MMM::Common::Config;

use strict;
use warnings FATAL => 'all';
use English qw( NR );
use Log::Log4perl qw(:easy);

use List::Util qw(first);
use File::stat qw();

our $VERSION = '0.01';

# TODO remember which config file was read

our $RULESET = {
	'this'					=> { 'required' => ['AGENT', 'TOOLS'], 'refvalues' => 'host' },
	'debug'					=> { 'default' => 0, 'boolean' => 1 },
	'active_master_role'	=> { 'required' => ['AGENT', 'MONITOR'], 'refvalues' => 'role' },
	'max_kill_retries'		=> { 'default' => 10, 'required' => ['AGENT'] },
	'default_copy_method'	=> { 'required' => ['TOOLS'], 'refvalues' => 'copy_method' },
	'clone_dirs'			=> { 'required' => ['TOOLS'], 'multiple' => 1 },
	'role'					=> { 'required' => ['AGENT', 'MONITOR'], 'multiple' => 1, 'section' => {
		'mode'					=> { 'required' => ['MONITOR'], 'values' => ['balanced', 'exclusive'] },
		'hosts'					=> { 'required' => ['MONITOR'], 'refvalues' => 'host', 'multiple' => 1 },
		'ips'					=> { 'required' => ['AGENT', 'MONITOR'], 'multiple' => 1 },
		'prefer'				=> { 'refvalues' => 'hosts' }
		}
	},
	'monitor'				=> { 'required' => ['MONITOR', 'CONTROL'], 'section' => {
		'ip'					=> { 'required' => ['MONITOR', 'CONTROL'] },
		'port'					=> { 'default' => '9988' },
		'pid_path'				=> { 'required' => ['MONITOR'] },
		'bin_path'				=> { 'required' => ['MONITOR'] },
		'status_path'			=> { 'required' => ['MONITOR'] },
		'ping_interval'			=> { 'default' => 1 },
		'ping_ips'				=> { 'required' => ['MONITOR'], 'multiple' => 1 },
		'flap_duration'			=> { 'default' => 60 * 60 },
		'flap_count'			=> { 'default' => 3 },
		'auto_set_online'		=> { 'default' => 0 },
		'kill_host_bin'			=> { 'default' => 'kill_host' },
		'careful_startup'		=> { 'default' => 1, 'boolean' => 1 },
		'mode'					=> { 'default' => 'active', 'values' => ['passive', 'active', 'manual', 'wait'] },
		'wait_for_other_master'	=> { 'default' => 120 }
		}
	},
	'socket'				=> { 'create_if_empty' => ['AGENT', 'CONTROL', 'MONITOR'], 'section' => {
		'type'					=> { 'default' => 'plain', 'required' => ['AGENT', 'CONTROL', 'MONITOR'], 'values' => [ 'plain', 'ssl' ] },
		'cert_file'				=> { 'deprequired' => { 'type' => 'ssl' }, 'required' => [ 'AGENT', 'CONTROL', 'MONITOR'] },
		'key_file'				=> { 'deprequired' => { 'type' => 'ssl' }, 'required' => [ 'AGENT', 'CONTROL', 'MONITOR'] },
		'ca_file'				=> { 'deprequired' => { 'type' => 'ssl' }, 'required' => [ 'AGENT', 'MONITOR'] }
		}
	},
	'copy_method'			=> { 'required' => ['TOOLS'], 'multiple' => 1, 'template' => 'default', 'section' => {
		'backup_command'		=> { 'required' => 1 },
		'restore_command'		=> { 'required' => 1 },
		'incremental_command'	=> { 'deprequired' => { 'incremental' => 1 } },
		'incremental'			=> { 'default' => 0, 'boolean' => 1 },
		'single_run'			=> { 'default' => 0, 'boolean' => 1 },
		'true_copy'				=> { 'default' => 0, 'boolean' => 1 },
		}
	},
	'host'					=> { 'required' => 1, 'multiple' => 1, 'template' => 'default', 'section' => {
		'ip'					=> { 'required' => ['AGENT', 'MONITOR', 'TOOLS'] },
		'mode'					=> { 'required' => ['AGENT', 'MONITOR'], 'values' => ['master', 'slave'] },
		'peer'					=> { 'deprequired' => { 'mode' => 'master' }, 'refvalues' => 'host' },

		'pid_path'				=> { 'required' => ['AGENT'] },
		'bin_path'				=> { 'required' => ['AGENT'] },
		'agent_port'			=> { 'default' => 9989 },
		'cluster_interface'		=> { 'required' => ['AGENT'] },

		'mysql_port'			=> { 'default' => 3306 },
		'mysql_pidfile'			=> { 'default' => '/var/run/mysqld/mysqld.pid' },
		'mysql_rcscript'		=> { 'default' => '/etc/init.d/mysql' },
		'mysql_cnf'				=> { 'default' => '/etc/my.cnf' },

		'agent_user'			=> { 'required' => ['AGENT'] },
		'agent_password'		=> { 'required' => ['AGENT'] },

		'monitor_user'			=> { 'required' => ['MONITOR'] },
		'monitor_password'		=> { 'required' => ['MONITOR'] },

		'replication_user'		=> { 'required' => ['AGENT', 'TOOLS'] },
		'replication_password'	=> { 'required' => ['AGENT', 'TOOLS'] },

		'ssh_user'				=> { 'required' => ['TOOLS'] },
		'ssh_port'				=> { 'default' => 22 },
		'ssh_parameters'		=> { 'default' => '' },
		'tools_user'			=> { 'required' => ['TOOLS'] },
		'tools_password'		=> { 'required' => ['TOOLS'] },

		'backup_dir'			=> { 'required' => ['TOOLS'] },
		'restore_dir'			=> { 'required' => ['TOOLS'] },

		'lvm_bin_lvcreate'		=> { 'default' => 'lvcreate' },
		'lvm_bin_lvremove'		=> { 'default' => 'lvremove' },
		'lvm_snapshot_size'		=> { 'required' => ['TOOLS'] },
		'lvm_logical_volume'	=> { 'required' => ['TOOLS'] },
		'lvm_volume_group'		=> { 'required' => ['TOOLS'] },
		'lvm_mount_dir'			=> { 'required' => ['TOOLS'] },
		'lvm_mount_opts'		=> { 'required' => ['TOOLS'] },
		}
	},
	'check'					=> { 'create_if_empty' => ['MONITOR'], 'multiple' => 1, 'template' => 'default', 'values' => ['ping', 'mysql', 'rep_backlog', 'rep_threads'], 'section' => {
		'check_period'			=> { 'default' => 5 },
		'trap_period'			=> { 'default' => 10 },
		'timeout'				=> { 'default' => 2 },
		'restart_after'			=> { 'default' => 10000 },
		'max_backlog'			=> { 'default' => 60 }		# XXX ugly
		}
	}
};


#-------------------------------------------------------------------------------
sub new($) {
	my $self = shift;

	return bless { }, $self; 
}

#-------------------------------------------------------------------------------
sub read($$) {
	my $self = shift;
	my $file = shift;
	my $fullname = $self->_get_filename($file);
	LOGDIE "Could not find a readable config file" unless $fullname;
	DEBUG "Loading configuration from $fullname";

	my $st = File::stat::stat($fullname);
	LOGDIE sprintf("Configuration file %s is world writable!", $fullname) if ($st->mode & 0002);
	LOGDIE sprintf("Configuration file %s is world readable!", $fullname) if ($st->mode & 0004);

	my $fd;
	open($fd, "<$fullname") || LOGDIE "Can't read config file '$fullname'";
	my $ret = $self->parse($RULESET, $fullname, $fd);
	close($fd);
	return $ret;
}

#-------------------------------------------------------------------------------
sub parse(\%\%$*); # needed because parse is a recursive function
sub parse(\%\%$*) {
	my $config	= shift;
	my $ruleset = shift;
	my $file	= shift;	# name of file
	my $fd		= shift;
	my $line;
	
	while ($line = <$fd>) {
		chomp($line);

		# comments and empty lines handling
		next if ($line =~ /^\s*#/ || $line =~ /^\s*$/);

		# end tag
		return if ($line =~ /^\s*<\/\s*(\w+)\s*>\s*$/);

		if ($line =~ /^\s*include\s+(\S+)\s*$/) {
			my $include_file = $1;
			$config->read($include_file);
			next;
		}

		# start tag - unique section
		if ($line =~/^\s*<\s*(\w+)\s*>\s*$/) {
			my $type = $1;
			if (!defined($ruleset->{$type}) || !defined($ruleset->{$type}->{section})) {
				LOGDIE "Invalid section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			if ($ruleset->{$type}->{multiple}) {
				LOGDIE "No section name specified for named section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			$config->{$type} = {} unless $config->{$type};
			parse(%{$config->{$type}}, %{$ruleset->{$type}->{section}}, $file, $fd);
			next;
		}
		# empty tag - unique section
		if ($line =~/^\s*<\s*(\w+)\s*\/>\s*$/) {
			my $type = $1;
			if (!defined($ruleset->{$type}) || !defined($ruleset->{$type}->{section})) {
				LOGDIE "Invalid section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			if ($ruleset->{$type}->{multiple}) {
				LOGDIE "No section name specified for named section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			$config->{$type} = {}			unless $config->{$type};
			next;
		}
		# start tag - named section
		if ($line =~/^\s*<\s*(\w+)\s+([\w\-_]+)\s*>\s*$/) {
			my $type = $1;
			my $name = $2;
			if (!defined($ruleset->{$type}) || !defined($ruleset->{$type}->{section})) {
				LOGDIE "Invalid section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			if (!$ruleset->{$type}->{multiple}) {
				LOGDIE "Section name specified for unique section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			$config->{$type} = {}			unless $config->{$type};
			$config->{$type}->{$name} = {}	unless $config->{$type}->{$name};
			parse(%{$config->{$type}->{$name}}, %{$ruleset->{$type}->{section}}, $file, $fd);
			next;
		}

		# empty tag - named section
		if ($line =~/^\s*<\s*(\w+)\s+([\w\-_]+)\s*\/>\s*$/) {
			my $type = $1;
			my $name = $2;
			if (!defined($ruleset->{$type}) || !defined($ruleset->{$type}->{section})) {
				LOGDIE "Invalid section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			if (!$ruleset->{$type}->{multiple}) {
				LOGDIE "Section name specified for unique section $type in '$file' on line $INPUT_LINE_NUMBER!";
			}
			$config->{$type} = {}			unless $config->{$type};
			$config->{$type}->{$name} = {}	unless $config->{$type}->{$name};
			next;
		}
		
		if ($line =~/^\s*(\S+)\s+(.*)$/) {
			my $var = $1;
			my $val = $2;
			LOGDIE "Unknown variable $var in '$file' on line $INPUT_LINE_NUMBER!" unless defined($ruleset->{$var});
			LOGDIE "'$var' should be a section instead of a variable in '$file' on line $INPUT_LINE_NUMBER!" if defined($ruleset->{$var}->{section});
			$val =~ s/\s+$//;
			@{$config->{$var}} = split(/\s*,\s*/, $val) if ($ruleset->{$var}->{multiple});
			$config->{$var} = $val unless ($ruleset->{$var}->{multiple});
			if ($ruleset->{$var}->{boolean}) {
				$ruleset->{$var}->{values} = [0, 1];
				if ($config->{$var} =~ /^(false|off|no|0)$/i) {
					$config->{$var} = 0;
				}
				elsif ($config->{$var} =~ /^(true|on|yes|1)$/i) {
					$config->{$var} = 1;
				}
			}
			next;
		}

		LOGDIE "Invalid config line in file '$file' on line $INPUT_LINE_NUMBER!";
	}
}

#-------------------------------------------------------------------------------
sub _get_filename($$) {
	my $self = shift;
	my $file = shift;

	$file .= '.conf' unless ($file =~ /\.conf$/);
	my @paths = qw(/etc /etc/mmm /etc/mysql-mmm);

	my $fullname;
	foreach my $path (@paths) {
		if (-r "$path/$file") {
			$fullname = "$path/$file";
			last;
		}
	}
	FATAL "No readable config file $file in ", join(', ', @paths) unless $fullname;
	return $fullname;
}

#-------------------------------------------------------------------------------
sub check($$) {
	my $self = shift;
	my $program = shift;
	$self->_check_ruleset('', $program, $RULESET, $self);
}

#-------------------------------------------------------------------------------
sub _check_ruleset(\%$$\%\%) {
	my $self	= shift;
	my $posstr	= shift;
	my $program	= shift;
	my $ruleset	= shift;
	my $config	= shift;

	foreach my $varname (keys(%{$ruleset})) {
		$self->_check_rule($posstr . $varname, $program, $ruleset, $config, $varname);
	}
}

#-------------------------------------------------------------------------------
sub _check_rule(\%$$\%\%$) {
	my $self	= shift;
	my $posstr	= shift;
	my $program	= shift;
	my $ruleset	= shift;
	my $config	= shift;
	my $varname	= shift;

	my $cur_rule = \%{$ruleset->{$varname}};

	# set default value if not defined
	if (!defined($config->{$varname}) && defined($cur_rule->{default})) {
		DEBUG "Undefined value for '$posstr', using default value '$cur_rule->{default}'";
		$config->{$varname} = $cur_rule->{default};
	}

	# check required
	if (defined($cur_rule->{required}) && defined($cur_rule->{deprequired})) {
		LOGDIE "Default value specified for required config entry '$posstr'" if defined($cur_rule->{default});
		LOGDIE "Invalid ruleset '$posstr' - deprequired should be a hash" if (ref($cur_rule->{deprequired}) ne "HASH");
		$cur_rule->{required} = _eval_program_condition($program, $cur_rule->{required}) if (ref($cur_rule->{required}) eq "ARRAY");
		my ($var, $val) = %{ $cur_rule->{deprequired} };
		# TODO WARN if field $var has a default value - this may not be evaluated yet.
		if (!defined($config->{$varname}) && $cur_rule->{required} == 1 && defined($config->{$var}) && $config->{$var} eq $val) {
			# TODO better error message for missing sections
			FATAL "Config entry '$posstr' is required because of '$var $val', but missing";
		}
	}
	elsif (defined($cur_rule->{required})) {
		$cur_rule->{required} = _eval_program_condition($program, $cur_rule->{required}) if (ref($cur_rule->{required}) eq "ARRAY");
		if (!defined($config->{$varname}) && $cur_rule->{required} == 1) {
			# TODO better error message for sections
			LOGDIE "Required config entry '$posstr' is missing";
			return;
		}
	}
	elsif (defined($cur_rule->{deprequired})) {
		LOGDIE "Invalid ruleset '$posstr' - deprequired should be a hash" if (ref($cur_rule->{deprequired}) ne "HASH");
		my ($var, $val) = %{ $cur_rule->{deprequired} };
		# TODO WARN if field $var has a default value - this may not be evaluated yet.
		if (!defined($config->{$varname}) && defined($config->{$var}) && $config->{$var} eq $val) {
			# TODO better error message for missing sections
			LOGDIE "Config entry '$posstr' is required because of '$var $val', but missing";
			return;
		}
	}

	return if (!defined($config->{$varname}) && !$cur_rule->{multiple});

	# handle sections
	if (defined($cur_rule->{section})) {

		# unique secions
		unless ($cur_rule->{multiple}) {
			# check variables of unique sections
			$self->_check_ruleset($posstr . '->', $program, $cur_rule->{section}, $config->{$varname});
			return;
		}

		# named sections ...

		# check if section name is one of the allowed
		if (defined($cur_rule->{values})) {
			my @allowed = @{$cur_rule->{values}};
			push @allowed, $cur_rule->{template} if defined($cur_rule->{template});
			foreach my $key (keys %{ $config->{$varname} }) {
				unless (defined(first { $_ eq $key; } @allowed)) {
					LOGDIE "Invalid $posstr '$key' in configuration allowed values are: '", join("', '", @allowed), "'"
				}
			}
		}

		# handle "create if empty"
		if (defined($cur_rule->{create_if_empty})) {
			$cur_rule->{create_if_empty} = _eval_program_condition($program, $cur_rule->{create_if_empty}) if (ref($cur_rule->{create_if_empty}) eq "ARRAY");
			if ($cur_rule->{create_if_empty} == 1) {
				$config->{$varname} = {} unless defined($config->{$varname});
				foreach my $value (@{$cur_rule->{values}}) {
					next if (defined($config->{$varname}->{$value}));
					$config->{$varname}->{$value} = {};
				}
			}
		}

		# handle section template
		if (defined($cur_rule->{template}) && defined($config->{$varname}->{ $cur_rule->{template} })) {
			my $template = $config->{$varname}->{ $cur_rule->{template} };	
			delete($config->{$varname}->{ $cur_rule->{template} });
			foreach my $var ( keys( %{ $template } ) ) {
				foreach my $key ( keys( %{ $config->{$varname} } ) ) {
					if (!defined($config->{$varname}->{$key}->{$var})) {
						$config->{$varname}->{$key}->{$var} = $template->{$var};
					}
				}
			}
		}

		# check variables of each named section
		foreach my $key ( keys( %{ $config->{$varname} } ) ) {
			$self->_check_ruleset($posstr . '->' . $key . '->', $program, $cur_rule->{section}, $config->{$varname}->{$key});
		}
		return;
	}

	# skip if undefined
	return if (!defined($config->{$varname}));
	
	# check if variable has one of the allowed values
	if (defined($cur_rule->{values}) || defined($cur_rule->{refvalues})) {
		my @allowed;
		if (defined($cur_rule->{values})) {
			@allowed = @{$cur_rule->{values}};
		}
		elsif (defined($cur_rule->{refvalues})) {
			if (defined($ruleset->{ $cur_rule->{refvalues} })) {
				# reference to section on current level
				my $reftype = ref($config->{ $cur_rule->{refvalues} });
				if ($reftype eq 'HASH') {
					@allowed = keys( %{ $config->{ $cur_rule->{refvalues} } } );
					# remove template section from list of valid values
					if (defined($ruleset->{ $cur_rule->{refvalues} }->{template})) {
						@allowed = grep { $_ ne $ruleset->{ $cur_rule->{refvalues} }->{template}} @allowed;
					}
				}
				elsif ($reftype eq 'ARRAY') {
					@allowed = @{ $config->{ $cur_rule->{refvalues} } };
				}
				else {
					return unless (ref($config->{ $cur_rule->{refvalues} }) eq "HASH");
#					LOGDIE "Could not find any $cur_rule->{refvalues}-sections";
				}
			}
			elsif (defined($RULESET->{ $cur_rule->{refvalues} })) {
				# reference to section on top level
				my $reftype = ref($self->{ $cur_rule->{refvalues} });
				if ($reftype eq 'HASH') {
					@allowed = keys( %{ $self->{ $cur_rule->{refvalues} } } );
					# remove template section from list of valid values
					if (defined($RULESET->{ $cur_rule->{refvalues} }->{template})) {
						@allowed = grep { $_ ne $RULESET->{ $cur_rule->{refvalues} }->{template}} @allowed;
					}
				}
				elsif ($reftype eq 'ARRAY') {
					@allowed = @{ $self->{ $cur_rule->{refvalues} } };
				}
				else {
					return unless (ref($self->{ $cur_rule->{refvalues} }) eq "HASH");
#					LOGDIE "Could not find any $cur_rule->{refvalues}-sections" unless (ref($self->{ $cur_rule->{refvalues} }) eq "HASH");
				}
			}
			else {
				LOGDIE "Invalid reference to non-section '$cur_rule->{refvalues}' for '$posstr'";
				return;
			}
		}
		if ($cur_rule->{multiple}) {
			for my $val ( @{ $config->{$varname} } ) {
				unless (defined(first { $_ eq $val; } @allowed)) {
					LOGDIE "Config entry '$posstr' has invalid value '$val' allowed values are: '", join("', '", @allowed), "'";
					return;
				}
			}
			return;
		}
		unless (defined(first { $_ eq $config->{$varname}; } @allowed)) {
			LOGDIE "Config entry '$posstr' has invalid value '$config->{$varname}' allowed values are: '", join("', '", @allowed), "'";
			return;
		}
	}
}

#-------------------------------------------------------------------------------
sub _eval_program_condition($$) {
	my $program = shift;
	my $value = shift;
	
	return 1 unless ($program);
	return 1 if (first { $_ eq $program; } @{ $value });
	return -1;
}

1;
