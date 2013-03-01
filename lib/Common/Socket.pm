package MMM::Common::Socket;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use IO::Socket::INET;

our $VERSION = '0.01';


=head1 NAME

MMM::Common::Socket - functions for socket creation

=cut


=head1 FUNCTIONS

=over 4

=item create_listener($host, $port)

Create a listening (ssl) socket on $host:$port.

=cut

sub create_listener($$) {
	my $host = shift;
	my $port = shift;

	my $socket_class = 'IO::Socket::INET';
	my %socket_opts;
	my $err = sub {''};
	if (defined($main::config->{'socket'}) && $main::config->{'socket'}->{type} eq 'ssl') {
 		require IO::Socket::SSL;
		$socket_class = 'IO::Socket::SSL';
		%socket_opts = (
			SSL_cert_file	=> $main::config->{'socket'}->{cert_file},
			SSL_key_file	=> $main::config->{'socket'}->{key_file},
			SSL_ca_file		=> $main::config->{'socket'}->{ca_file},
			SSL_verify_mode	=> 0x03
		);
		$err = sub {"\n  ", IO::Socket::SSL::errstr()};
	}

	my $sock = $socket_class->new(
		LocalHost => $host,
		LocalPort => $port, 
		Proto => 'tcp', 
		Listen => 10, 
		Reuse => 1,
		%socket_opts,
	) or LOGDIE "Listener: Can't create socket!", $err->();


	$sock->timeout(3);
	return($sock);
}

=item create_sender($host, $port, $timeout)

Create a (ssl) client socket on $host:$port with timeout $timeout.

=cut

sub create_sender($$$) {
	my $host = shift;
	my $port = shift;
	my $timeout = shift;

	my $socket_class = 'IO::Socket::INET';
	my %socket_opts;

	if (defined($main::config->{'socket'}) && $main::config->{'socket'}->{type} eq "ssl") {
		require IO::Socket::SSL;
		$socket_class = 'IO::Socket::SSL';
		%socket_opts = (
			SSL_use_cert	=> 1,
			SSL_cert_file	=> $main::config->{'socket'}->{cert_file},
			SSL_key_file	=> $main::config->{'socket'}->{key_file},
			SSL_ca_file		=> $main::config->{'socket'}->{ca_file},
		);
	}

	return $socket_class->new(
		PeerAddr	=> $host,
		PeerPort	=> $port,
		Proto		=> 'tcp',
		($timeout ? (Timeout => $timeout) : ()),
		%socket_opts,
	);
}

1;
