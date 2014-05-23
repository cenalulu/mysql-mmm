package MMM::Common::PidFile;

use strict;
use warnings FATAL => 'all';
use English qw( PROCESS_ID );
use Log::Log4perl qw(:easy);

our $VERSION = '0.01';

sub new($$) { 
	my $self = shift;
	my $path = shift;

	return bless { 'path' => $path }, $self; 
}

sub exists($) {
	my $self = shift;
	return -f $self->{path};
}

sub is_running($) {
	my $self = shift;

	return 0 unless $self->exists();

	open(PID, $self->{path}) || LOGDIE "Can't open pid file '$self->{path}' for reading!\n";
	chomp(my $pid = <PID>);
	close(PID);

	return kill(0, $pid);
}

sub create($) {
	my $self = shift;

	open(PID, ">" . $self->{path}) || LOGDIE "Can't open pid file '$self->{path}' for writing!\n";
	print PID $PROCESS_ID;
	close(PID);

	DEBUG "Created pid file '$self->{path}' with pid $PROCESS_ID";
}

sub remove($) {
	my $self = shift;
	
	unlink $self->{path};
}

1;

__END__

=head1 NAME

MMM::Common::Pidfile - Manage process id files

=cut


=head1 SYNOPSIS

	my $pidfile = new MMM::Common::PidFile:: '/path/to/your.pid';

	# create pidfile with current process id
	$pidfile->create();

	# check if pidfile exists
	$pidfile->exists();

	# check if the process with the process id from the pidfile is still running
	$pidfile->is_running();

	# remove pidfile
	$pidfile->remove();

=cut

