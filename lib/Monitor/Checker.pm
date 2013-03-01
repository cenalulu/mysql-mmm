package MMM::Monitor::Checker;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use IPC::Open2;
use MMM::Monitor::CheckResult;

our $VERSION = '0.01';

=head1 NAME

MMM::Monitor::Checker - Checker class and main function for the checker threads

=head1 SYNOPSIS

	# Spawn a checker thread which will create and poll a checker and push its status to $queue
	our $shutdown :shared = 0;
	$SIG{INT} = sub { $shutdown = 1; };
	my $queue = checker_queue(new Thread::Queue::)
	my $ping_thread  = new threads(\&MMM::Monitor::Checker::main, 'ping', $queue)
	my $mysql_thread = new threads(\&MMM::Monitor::Checker::main, 'mysql', $queue)
	...

=cut

sub main($$) {
	my $check_name	= shift;
	my $queue		= shift;

	# Some shortcuts
	my @checks		= keys(%{$main::config->{check}});
	my @hosts		= keys(%{$main::config->{host}});
	my $options		= $main::config->{check}->{$check_name};

	# Create checker
	my $checker = new MMM::Monitor::Checker::($check_name);

	# Initialize failure counters
	my $failures = {};
	foreach my $host_name (@hosts) {
		$failures->{$host_name} = {
			state	=> -1,				# -1 undefined; 1 - ok; -2 untrapped error; 0 trapped error
			time	=> 0,
		}
	}

	# Perform checks until shutdown
	while (!$main::shutdown) {
		foreach my $host_name (@hosts) {
			last if ($main::shutdown);
			last unless ($main::have_net);

			# Ping checker
			$checker->spawn() unless $checker->ping();

			# Check service ...
			my $res = $checker->check($host_name);

			# If success
			if ($res =~ /^OK/) {
				next if ($failures->{$host_name}->{state} == 1);
				if ($failures->{$host_name}->{state} != -2) {
					INFO "Check '$check_name' on '$host_name' is ok!";
					$queue->enqueue(new MMM::Monitor::CheckResult::($host_name, $check_name, 1, $res));
				}
				$failures->{$host_name}->{time}		= 0;
				$failures->{$host_name}->{state}	= 1;
				next;
			}

			# If unknown
			if ($res =~ /^UNKNOWN/) {
				next if ($failures->{$host_name}->{state} == -3);
				$failures->{$host_name}->{time} = time();
				$failures->{$host_name}->{state}= -3;
				WARN "Check '$check_name' on '$host_name' is in unknown state! Message: $res";
				next;
			}
			
			# If failed
			if ($res =~ /^ERROR/) {
				last unless ($main::have_net);
				next if ($failures->{$host_name}->{state} == 0);
				if ($failures->{$host_name}->{state} != 0 && $failures->{$host_name}->{state} != -2) {
					$failures->{$host_name}->{time} = time();
					$failures->{$host_name}->{state}= -2;
				}
				my $failure_age = time() - $failures->{$host_name}->{time};
				
				next if ($failure_age < $options->{trap_period});

				ERROR "Check '$check_name' on '$host_name' has failed for $failure_age seconds! Message: $res";
				$queue->enqueue(new MMM::Monitor::CheckResult::($host_name, $check_name, 0, $res));
				$failures->{$host_name}->{state}	= 0;
				next;
			}
		}

		sleep($options->{check_period});
	}
	$checker->shutdown();
}


=pod

	# Create checker - will spawn a checker process
	my $checker = new MMM::Monitor::Checker::('ping');

=cut

sub new($$) {
	my $class	= shift;
	my $name	= shift;

	my $self = {};

	$self->{name} = $name;
	bless $self, $class; 
	$self->spawn();
	return $self;
}


=pod

	# Respawn checker if it doesn't respond
	$checker->spawn() unless $checker->ping();

=cut

sub spawn($) {
	my $self	= shift;
	my $name	= $self->{name};

	my $reader;		# STDOUT of checker
	my $writer;		# STDIN  of checker

	INFO "Spawning checker '$name'...";

	my $cluster = ($main::cluster_name ? '@' . $main::cluster_name : '');
	my $pid = open2($reader, $writer, $main::config->{monitor}->{bin_path} . "/monitor/checker $cluster $name");
	if (!$pid) {
		LOGDIE "Can't spawn checker! Error: $!";
	}

	$self->{pid}	= $pid;
	$self->{reader}	= $reader;
	$self->{writer}	= $writer;
}


=pod

	# Shutdown checker process
	$checker->shutdown();

=cut

sub shutdown($) {
	my $self	= shift;
	my $name	= $self->{name};

	INFO "Shutting down checker '$name'...";

	my $reader = $self->{reader};
	my $writer = $self->{writer};

	my $send_res = print $writer "quit\n";
	my $recv_res = <$reader>;
	chomp($recv_res) if defined($recv_res);
}


=pod

	# Check if checker process is still alive
	$checker->ping();

=cut

sub ping($) {
	my $self	= shift;
	my $name	= $self->{name};

#	DEBUG "Pinging checker '$name'...";

	my $reader = $self->{reader};
	my $writer = $self->{writer};
	
	my $send_res = print $writer "ping\n";
	my $recv_res;
READ: {
	$recv_res = <$reader>;
	redo READ if !$recv_res && $!{EINTR};
}
	chomp($recv_res) if defined($recv_res);

	if (!$send_res || !$recv_res || !($recv_res =~ /^OK/)) {
		WARN "Checker '$name' is dead!";
		return 0;
	}

#	DEBUG "Checker '$name' is OK ($recv_res)";
	return 1;
}


=pod

	# Tell the checker to check host 'db2'
	$checker->check('db2');

=cut

sub check($$) {
	my $self	= shift;
	my $host	= shift;

	my $name	= $self->{name};

	my $reader = $self->{reader};
	my $writer = $self->{writer};
	
	my $send_res = print $writer "check $host\n";
	my $recv_res;
READ: {
	$recv_res = <$reader>;
	redo READ if !$recv_res && $!{EINTR};
}
	chomp($recv_res) if defined($recv_res);

	return "UNKNOWN: Checker '$name' is dead!" unless ($send_res && $recv_res);
	return $recv_res;
}

1;
