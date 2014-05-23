package MMM::Common::Log;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use English qw( PROGRAM_NAME );

our $VERSION = '0.01';


sub init($$) {
	my $file = shift;
	my $progam = shift;

	my @paths = qw(/etc /etc/mmm /etc/mysql-mmm);

	# Determine filename
	my $fullname;
	foreach my $path (@paths) {
		if (-r "$path/$file") {
			$fullname = "$path/$file";
			last;
		}
	}

	# Read configuration from file
	if ($fullname) {
		Log::Log4perl->init($fullname);
		return;
	}

	# Use default configuration
	my $conf = "
		log4perl.logger = INFO, LogFile

		log4perl.appender.LogFile                           = Log::Log4perl::Appender::File
		log4perl.appender.LogFile.Threshold                 = INFO 
		log4perl.appender.LogFile.filename                  = /var/log/mysql-mmm/$progam.log
		log4perl.appender.LogFile.recreate                  = 1
		log4perl.appender.LogFile.layout                    = PatternLayout
		log4perl.appender.LogFile.layout.ConversionPattern  = %d %5p %m%n
	";
	Log::Log4perl->init(\$conf);

}

sub debug() {
	my $stdout_appender =  Log::Log4perl::Appender->new(
		'Log::Log4perl::Appender::Screen',
		name      => 'ScreenLog',
		stderr    => 0
	);
	my $layout = Log::Log4perl::Layout::PatternLayout->new('%d %5p %m%n');
	$stdout_appender->layout($layout);
	Log::Log4perl::Logger->get_root_logger()->add_appender($stdout_appender);
	Log::Log4perl::Logger->get_root_logger()->level($DEBUG);
}

1;
