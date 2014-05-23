package MMM::Common::Uptime;

use strict;
use warnings FATAL => 'all';
use English qw( OSNAME );
use Log::Log4perl qw(:easy);

require Exporter;
our @ISA = qw( Exporter );
our @EXPORT_OK = qw( uptime );

our $VERSION = '0.01';

# FIXME Solaris

if ($OSNAME eq 'linux') {
	use constant UPTIME => "/proc/uptime";
}
else {
	LOGDIE "Unsupported platform - can't get uptime!";
}

sub uptime {
	if ($OSNAME eq 'linux') {
		DEBUG "Fetching uptime from ", UPTIME;
		open(FILE, UPTIME) || LOGDIE "Unable to get uptime from ", UPTIME;
		my $line = <FILE>;
		my ($uptime, $idle) = split(/\s+/, $line);
		close(FILE);

		DEBUG "Uptime is ", $uptime;
		return $uptime;
	}

	LOGDIE "Unsupported platform - can't get uptime!";
}

1;
