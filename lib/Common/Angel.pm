package MMM::Common::Angel;

use strict;
use warnings FATAL => 'all';
use English qw( CHILD_ERROR ERRNO );
use Log::Log4perl qw(:easy);
use Errno qw( EINTR );
use POSIX qw( WIFEXITED WIFSIGNALED WEXITSTATUS WTERMSIG WNOHANG );


our $start_process;
our $pid;
our $attempts;
our $starttime;


sub Init($) { 
	my $pidfile = shift;

	
	$MMM::Common::Angel::start_process	= 1;
	$MMM::Common::Angel::attempts		= 0;
	$MMM::Common::Angel::starttime		= time();
	my $is_shutdown	= 0;

	$pidfile->create() if (defined($pidfile));

	local $SIG{INT}		= \&MMM::Common::Angel::SignalHandler;
	local $SIG{TERM}	= \&MMM::Common::Angel::SignalHandler;
	local $SIG{QUIT}	= \&MMM::Common::Angel::SignalHandler;

	do {
		$MMM::Common::Angel::attempts++;

		if ($MMM::Common::Angel::start_process) {
			$MMM::Common::Angel::start_process = 0;

			# Create a new child
			$MMM::Common::Angel::pid = fork();

			# Die if we couldn't fork
			LOGDIE "Couldn't fork child process." unless (defined($MMM::Common::Angel::pid));

			# Return if we are the child
			return if ($MMM::Common::Angel::pid == 0);
		}

		# Wait for child to exit
		if (waitpid($MMM::Common::Angel::pid, 0) == -1) {
			if ($ERRNO{ECHLD}) {
				$is_shutdown = 1 unless ($MMM::Common::Angel::start_process);
			}
		}
		else {
			if (WIFEXITED($CHILD_ERROR)) {
				if (WEXITSTATUS($?) == 0) {
					INFO "Child exited normally (with exitcode 0), shutting down";
					$is_shutdown = 1;
				}
				else {
					my $now = time();
					my $diff = $now - $MMM::Common::Angel::starttime;
					if ($MMM::Common::Angel::attempts >= 10 && $diff < 300) {
						FATAL sprintf("Child exited with exitcode %s and has failed more than 10 times consecutively in the last 5 minutes, not restarting", WEXITSTATUS($?));
						$MMM::Common::Angel::start_process	= 0;
						$is_shutdown = 1;
					}
					else {
						FATAL sprintf("Child exited with exitcode %s, restarting after 10 second sleep", WEXITSTATUS($?));
						if ($diff >= 300 ) {
							# reset attempts and starttime
							$MMM::Common::Angel::attempts = 0;
							$MMM::Common::Angel::starttime = time();
						}
						sleep(10);
						$MMM::Common::Angel::start_process	= 1;
					}
				}
			}
			if (WIFSIGNALED($CHILD_ERROR)) {
				FATAL sprintf("Child exited with signal %s, restarting", WTERMSIG($?));
				$MMM::Common::Angel::start_process	= 1;
			}
		}

	} while (!$is_shutdown);

	
	$pidfile->remove() if (defined($pidfile));
	exit(0);
}

sub SignalHandler {
	my $signame = shift;
	kill ($signame, $MMM::Common::Angel::pid);
}

1;
