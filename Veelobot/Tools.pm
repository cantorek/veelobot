package Veelobot::Tools;

use strict;

use Data::Dumper;

use threads;

sub debug() {
    my ($self, $level, @txt ) = @_;
    print "Debug[".time()."|$level|".threads->tid()."]: " . join(" ", @txt) . "\r\n" if (defined($ENV{'DEBUG'}) && $ENV{'DEBUG'} >= $level);
}


sub get_domain() {
    my $self = shift;
    my $url = shift;

    my $domain = undef;
    my $proto = $self->get_proto($url) || return undef;

    $url =~ /$proto:\/\/([a-z0-9.-]+)([\/|?|&]|$)/i;
    $domain = $1;

    return $domain;

}

sub get_proto() {
    my $self = shift;
    my $url = shift;

    my $proto = undef;
    $url =~ /([a-z0-9]+):\/\//i;
    $proto = $1;

    return $proto;

}


1;
