package MMM::Agent::Helpers;

use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);

our $VERSION = 	'0.01';


=head1 NAME

MMM::Agent::Helpers - an interface to helper programs for B<mmm_agentd>

=cut


=head1 FUNCTIONS

=over 4

=item check_ip($if, $ip)

Check if the IP $ip is configured on interface $if.

Calls B<bin/agent/check_ip>.

=cut

sub check_ip($$) {
	my $if = shift;
	my $ip = shift;
	return _execute('check_ip', "$if $ip");
}


=item configure_ip($if, $ip)

Check if the IP $ip is configured on interface $if. If not, configure it and
send arp requests to notify other hosts.

Calls B<bin/agent/configure_ip>.

=cut

sub configure_ip($$) {
	my $if = shift;
	my $ip = shift;
	return _execute('configure_ip', "$if $ip");
}


=item clear_ip($if, $ip)

Remove the IP address $ip from interface $if.

Calls B<bin/agent/clear_ip>.

=cut

sub clear_ip($$) {
	my $if = shift;
	my $ip = shift;
	return _execute('clear_ip', "$if $ip");
}


=item mysql_may_write( )

Determine wheter writes on local MySQL server are allowes.

Calls B<bin/agent/mysql_may_write>, which reads the config file.

=cut

sub may_write() {
	return _execute('mysql_may_write');
}

=item mysql_allow_write( )

Allow writes on local MySQL server. Sets global read_only to 0.

Calls B<bin/agent/mysql_allow_write>, which reads the config file.

=cut

sub allow_write() {
	return _execute('mysql_allow_write');
}


=item mysql_deny_write( )

Deny writes on local MySQL server. Sets global read_only to 1.

Calls B<bin/agent/mysql_deny_write>, which reads the config file.

=cut

sub deny_write() {
	return _execute('mysql_deny_write');
}


=item turn_on_slave( )

Start slave on local MySQL server.

Calls B<bin/agent/turn_on_slave>, which reads the config file.

=cut

sub turn_on_slave() {
	return _execute('turn_on_slave');
}


=item turn_off_slave( )

Stop slave on local MySQL server.

Calls B<bin/agent/turn_off_slave>, which reads the config file.

=cut

sub turn_off_slave() {
	return _execute('turn_off_slave');
}


=item sync_with_master( )

Try to sync a (soon active) master up with his peer (old active master) when the
I<active_master_role> is moved. If peer is reachable sync with master log. If 
not reachable, sync with relay log.

Calls B<bin/agent/sync_with_master>, which reads the config file.

=cut

sub sync_with_master() {
	return _execute('sync_with_master');
}


=item set_active_master($new_master)

Try to catch up with the old master as far as possible and change the master to the new host.
(Syncs to the master log if the old master is reachable. Otherwise syncs to the relay log.)

Calls B<bin/agent/set_active_master>, which reads the config file.

=cut

sub set_active_master($) {
	my $new_master = shift;
	return "ERROR: Unknown host $new_master" unless (defined($main::config->{host}->{$new_master}));
	return _execute('set_active_master', $new_master);
}

#-------------------------------------------------------------------------------
sub _execute($$$) {
	my $command		= shift;
	my $params		= shift;
	my $return_all	= shift;

	my $path		= $main::agent->bin_path . "/agent/$command";
	my $config_file		= $main::agent->config_file;
	$params = '' unless defined($params);

	DEBUG "Executing $path $params";
	my $res = `$path $config_file $params`;

	unless ($return_all) {
		my @lines = split /\n/, $res;
		return pop(@lines);
	}
	
	return $res;
}

1;

=back
=cut

