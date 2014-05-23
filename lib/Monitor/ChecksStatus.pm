package MMM::Monitor::ChecksStatus;
use base 'Class::Singleton';

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use MMM::Monitor::Checker

our $VERSION = '0.01';

sub _new_instance($) {
	my $class = shift;

	my $data = {};

	my @checks		= keys(%{$main::config->{check}});
	my @hosts		= keys(%{$main::config->{host}});

	foreach my $host_name (@hosts) {
		$data->{$host_name} = {};
	}

	my $time = time();

	# Perform initial checks
	INFO 'Performing initial checks...';
	foreach my $check_name (@checks) {

		# Spawn checker
		my $checker = new MMM::Monitor::Checker::($check_name);

		# Check all hosts
		foreach my $host_name (@hosts) {
			DEBUG "Trying initial check '$check_name' on host '$host_name'";
			my $res = $checker->check($host_name);
			DEBUG "$check_name($host_name) = '$res'";
			$data->{$host_name}->{$check_name} = {};
			$data->{$host_name}->{$check_name}->{status}      = ($res =~ /^OK/)? 1 : 0;
			$data->{$host_name}->{$check_name}->{last_change} = $time;
			$data->{$host_name}->{$check_name}->{message}     = $res;
		}

		# Shutdown checker
		$checker->shutdown();
	}
	return bless $data, $class; 
}


=item handle_result(MMM::Monitor::CheckResult $result)

handle the results of a check and change state accordingly

=cut

sub handle_result($$) {
	my $self = shift;
	my $result = shift;

	# always save the latest message, but don't override time of last change
	$self->{$result->{host}}->{$result->{check}}->{message}     = $result->{message};
	return if ($result->{result} == $self->{$result->{host}}->{$result->{check}}->{status}); 

	$self->{$result->{host}}->{$result->{check}}->{status}      = $result->{result};
	$self->{$result->{host}}->{$result->{check}}->{last_change} = time();
}

=item ping($host)

Get state of check "ping" on host $host.

=cut

sub ping($$) {
	my $self = shift;
	my $host = shift;
	return $self->{$host}->{ping}->{status};
}


=item ping($host)

Get state of check "mysql" on host $host.

=cut

sub mysql($$) {
	my $self = shift;
	my $host = shift;
	return $self->{$host}->{mysql}->{status};
}


=item rep_threads($host)

Get state of check "rep_threads" on host $host.

=cut

sub rep_threads($$) {
	my $self = shift;
	my $host = shift;
	return $self->{$host}->{rep_threads}->{status};
}


=item rep_backlog($host)

Get state of check "rep_backlog" on host $host.

=cut

sub rep_backlog($$) {
	my $self = shift;
	my $host = shift;
	return $self->{$host}->{rep_backlog}->{status};
}


=item last_change($host, [$check])

Get time of last state change

=cut

sub last_change {
	my $self  = shift;
	my $host  = shift;
	my $check = shift || undef;

	return $self->{$host}->{$check}->{last_change} if (defined($check));

	my $time = $self->{$host}->{ping}->{last_change};
	$time = $self->{$host}->{mysql}->{last_change}       if ($self->{$host}->{mysql}->{last_change}       > $time);
	$time = $self->{$host}->{rep_threads}->{last_change} if ($self->{$host}->{rep_threads}->{last_change} > $time);
	$time = $self->{$host}->{rep_backlog}->{last_change} if ($self->{$host}->{rep_backlog}->{last_change} > $time);
	return $time;
}


=item message($host, $check)

Get time of last state change

=cut

sub message($$$) {
	my $self  = shift;
	my $host  = shift;
	my $check = shift;
	return $self->{$host}->{$check}->{message};
}

1;
