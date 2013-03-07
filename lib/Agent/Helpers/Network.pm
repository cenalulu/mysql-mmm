package MMM::Agent::Helpers::Network;

use strict;
use warnings FATAL => 'all';
use English qw( OSNAME );

our $VERSION = '0.01';

if ($OSNAME eq 'linux' || $OSNAME eq 'freebsd') {
	# these libs will always be loaded, use require and then import to avoid that
	use Net::ARP;
	use Time::HiRes qw( usleep );
}

=head1 NAME

MMM::Agent::Helpers::Network - network related functions for the B<mmm_agentd> helper programs

=cut


=head1 FUNCTIONS

=over 4

=item check_ip($if, $ip)

Check if the IP $ip is configured on interface $if. Returns 0 if not, 1 otherwise.

=cut

sub check_ip($$) {
	my $if = shift;
	my $ip = shift;

	my $output;
	if ($OSNAME eq 'linux') {
		$output = `/sbin/ip addr show dev $if`;
		_exit_error("Could not check if ip $ip is configured on $if: $output") if ($? >> 8 == 255);
	}
	elsif ($OSNAME eq 'solaris') {
		# FIXME $if is not used here
		$output = `/usr/sbin/ifconfig -a | grep inet`;
		_exit_error("Could not check if ip $ip is configured on $if: $output") if ($? >> 8 == 255);
	}
	elsif ($OSNAME eq 'freebsd') {
		$output = `/sbin/ifconfig $if | grep inet`;
		_exit_error("Could not check if ip $ip is configured on $if: $output") if ($? >> 8 == 255);
	}
	else {
		_exit_error("ERROR: Unsupported platform!");
	}

	return ($output =~ /\D+$ip\D+/) ? 1 : 0;
}


=item add_ip($if, $ip)

Add IP $ip to the interface $if.

=cut

sub add_ip($$) {
	my $if = shift;
	my $ip = shift;

	my $output;
	if ($OSNAME eq 'linux') {
		$output = `/sbin/ip addr add $ip/32 dev $if`;
		_exit_error("Could not configure ip $ip on interface $if: $output") if ($? >> 8 == 255);
	}
	elsif ($OSNAME eq 'solaris') {
		$output = `/usr/sbin/ifconfig $if addif $ip`;
		_exit_error("Could not configure ip $ip on interface $if: $output") if ($? >> 8 == 255);
		my $logical_if = _solaris_find_logical_if($ip);
		unless ($logical_if) {
			_exit_error("ERROR: Can't find logical interface with IP = $ip");
		}
		$output = `/usr/sbin/ifconfig $logical_if up`;
		_exit_error("Could not activate logical interface $logical_if with ip $ip on interface: $output") if ($? >> 8 == 255);
	}
	elsif ($OSNAME eq 'freebsd') {
		$output = `/sbin/ifconfig $if inet $ip netmask 255.255.255.255 alias`;
		_exit_error("Could not configure ip $ip on interface $if: $output") if ($? >> 8 == 255);
	}
	else {
		_exit_error("ERROR: Unsupported platform!");
	}
	return check_ip($if, $ip);
}


=item clear_ip($if, $ip)

Remove the IP $ip from the interface $if.

=cut

sub clear_ip($$) {
	my $if = shift;
	my $ip = shift;

	my $output;
	if ($OSNAME eq 'linux') {
		$output = `/sbin/ip addr del $ip/32 dev $if`;
		_exit_error("Could not remove ip $ip from interface $if: $output") if ($? >> 8 == 255);
	}
	elsif ($OSNAME eq 'solaris') {
		$output = `/usr/sbin/ifconfig $if removeif $ip`;
		_exit_error("Could not remove ip $ip from interface $if: $output") if ($? >> 8 == 255);
	}
	elsif ($OSNAME eq 'freebsd') {
		$output = `/sbin/ifconfig $if inet $ip -alias`;
		_exit_error("Could not remove ip $ip from interface $if: $output") if ($? >> 8 == 255);
	}
	else {
		exit(1);
	}
}


=item send_arp($if, $ip)

Send arp requests for the IP $ip to the broadcast address on network interface $if.

=cut

sub send_arp($$) {
	my $if = shift;
	my $ip = shift;


	if ($OSNAME eq 'linux' || $OSNAME eq 'freebsd') {
		my $mac = '';
		if ($Net::ARP::VERSION < 1.0) {
			Net::ARP::get_mac($if, $mac);
		}
		else {
			$mac = Net::ARP::get_mac($if);
		}
		return "ERROR: Couldn't get mac adress of interface $if" unless ($mac);

		for (my $i = 0; $i < 5; $i++) {
			Net::ARP::send_packet($if, $ip, $ip, $mac, 'ff:ff:ff:ff:ff:ff', 'request');
			usleep(50);
			Net::ARP::send_packet($if, $ip, $ip, $mac, 'ff:ff:ff:ff:ff:ff', 'reply');
			usleep(50) if ($i < 4);
		}
	}
	elsif ($OSNAME eq 'solaris') {
		# Get params for send_arp
		my $ipaddr = `/usr/sbin/ifconfig $if`;

		# Get broadcast address and netmask
		$ipaddr =~ /netmask\s*([0-9a-f]+)\s*broadcast\s*([\d\.]+)/i;
		my $if_bcast = $1;
		my $if_mask = $2;
		`/bin/send_arp -i 100 -r 5 -p /tmp/send_arp $if $ip auto $if_bcast $if_mask`;
	}
	else {
		_exit_error("ERROR: Unsupported platform!");
	}
}

sub _exit_error {
    my $msg = shift;

    print "ERROR: $msg\n"   if ($msg);
    print "ERROR\n"         unless ($msg);

    exit(255);
}

#-------------------------------------------------------------------------------
sub _solaris_find_logical_if($) {
	my $ip = shift;
	my $ifconfig = `/usr/sbin/ifconfig -a`;
	$ifconfig =~ s/\n/ /g;

	while ($ifconfig =~ s/([a-z0-9\:]+)(\:\s+.*?)inet\s*([0-9\.]+)//) {
		return $1 if ($3 eq $ip);
	}
	return undef;
}

1;
