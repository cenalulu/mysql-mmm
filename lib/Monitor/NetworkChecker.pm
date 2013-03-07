package MMM::Monitor::NetworkChecker;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use MMM::Monitor::Checker;

our $VERSION = '0.01';

=head1 NAME

MMM::Monitor::NetworkChecker - Function for checking state of network

=head1 SYNOPSIS

	our $shutdown :shared = 0;
	our $have_net :shared = 0;
	$SIG{INT} = sub { $shutdown = 1; };
	my $thread  = new threads(\&MMM::Monitor::NetworkChecker::main)

=cut
sub main() {
	my @ips		= @{$main::config->{monitor}->{ping_ips}};
	
	# Create checker
	my $checker = new MMM::Monitor::Checker::('ping_ip');

	# Perform checks until shutdown
	while (!$main::shutdown) {
		my $state = 0;

		foreach my $ip (@ips) {
			last if ($main::shutdown);

			# Ping checker
			$checker->spawn() unless $checker->ping();

			my $res = $checker->check($ip);
			if ($res =~ /^OK/) {
				$state = 1;
				last;
			}
		}

		if ($main::have_net != $state) {
			FATAL "Network is reachable"	if     ($state);
			FATAL "Network is unreachable"	unless ($state);
			$main::have_net = $state;
		}

		# Sleep a while before checking every ip again
		sleep($main::config->{monitor}->{ping_interval});
	}
	$checker->shutdown();
}

sub wait_for_network() {
	my @ips		= @{$main::config->{monitor}->{ping_ips}};
	
	# Create checker
	my $checker = new MMM::Monitor::Checker::('ping_ip');

	while (!$main::shutdown) {
		# Ping all ips
		foreach my $ip (@ips) {
			last if ($main::shutdown);
			# Ping checker
			$checker->spawn() unless $checker->ping();
	
			my $res = $checker->check($ip);
			if ($res =~ /^OK/) {
				DEBUG "IP '$ip' is reachable: $res";
				$checker->shutdown();
				return 1;
			}
		}

		# Sleep a while before checking every ip again
		sleep($main::config->{monitor}->{ping_interval});
	}
	$checker->shutdown();
	return 0;
}

1;
