package MMM::Common::Role;

use strict;
use warnings FATAL => 'all';

our $VERSION = '0.01';

use Class::Struct;

use overload
	'==' => \&is_equal_full,
	'eq' => \&is_equal_name,
	'!=' => sub { return !MMM::Common::Role::is_equal_full($_[0], $_[1]); },
	'ne' => sub { return !MMM::Common::Role::is_equal_name($_[0], $_[1]); },
	'cmp' => \&cmp,
	'""' => \&to_string;
		

struct 'MMM::Common::Role' => {
	name	=> '$',
	ip		=> '$',
};


#-------------------------------------------------------------------------------
# NOTE: takes a role object as param
sub is_equal_full($$) {
	my $self	= shift;
	my $other	= shift;
	
	return 0 if ($self->name ne $other->name);
	return 0 if ($self->ip   ne $other->ip);
	return 1;
}

#-------------------------------------------------------------------------------
# NOTE: takes a role object as param
sub is_equal_name($$) {
	my $self	= shift;
	my $other	= shift;
	
	return ($self->name eq $other->name);
}

sub cmp($$) {
	my $self	= shift;
	my $other	= shift;
	
	return ($self->name cmp $other->name) if ($self->name ne $other->name);
	return ($self->ip cmp $other->ip);
}

sub to_string($) {
	my $self	= shift;
	return sprintf('%s(%s)', $self->name, $self->ip);
}

sub from_string($$) {
	my $class	= shift;
	my $string	= shift;

	if (my ($name, $ip) = $string =~ /(.*)\((.*)\)/) {
		return $class->new(name => $name, ip => $ip);
	}
	return undef;
}

1;
