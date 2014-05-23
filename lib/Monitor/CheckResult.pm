package MMM::Monitor::CheckResult;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use threads;
use threads::shared;

our $VERSION = '0.01';

sub new($$$$$) {
	my $class	= shift;
	my $host	= shift;
	my $check	= shift;
	my $result	= shift;
	my $message	= shift;

	my %self :shared;
	$self{host}		= $host;
	$self{check}	= $check;
	$self{result}	= $result;
	$self{message}	= $message;
	return bless \%self, $class;
}

1;
