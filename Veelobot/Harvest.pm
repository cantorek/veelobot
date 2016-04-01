package Veelobot::Harvest;

use strict;

use Net::Curl::Easy qw(:constants);
use Net::Curl::Multi qw(:constants);

use threads;
use threads::shared;

use Thread::Queue;

use Veelobot::Tools;

use Data::Dumper;

#my $multi = undef;

my $data = Thread::Queue->new();

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $urls = shift; #queue
    my $data = shift;

    my $self = {};
#    share($self);
    my @handles;
    $self->{handles} = \@handles;
    $self->{urls} = $urls;

    $self->{multi} = undef;
    $self->{data} = $data;

    bless($self, $class);
    return($self);
}

sub add_handles() {
    my $self = shift;
    my $num_handles = shift;
    my $urls = $self->{urls};

    $self->{multi} = Net::Curl::Multi->new();
    $self->{multi}->setopt( Net::Curl::Multi::CURLMOPT_PIPELINING, 1);
    $self->{multi}->setopt( Net::Curl::Multi::CURLMOPT_MAXCONNECTS, 5000);

    Veelobot::Tools->debug(5, "Adding ", $num_handles, "handles");

    for(1..$num_handles) {
        my $easy = Net::Curl::Easy->new();
        $easy->setopt( Net::Curl::Easy::CURLOPT_VERBOSE(), 1 ) if ( $ENV{'DEBUG'} && $ENV{'DEBUG'} >= 99);
        $easy->setopt( Net::Curl::Easy::CURLOPT_FOLLOWLOCATION, 1 );
        $easy->setopt( Net::Curl::Easy::CURLOPT_MAXREDIRS, 5 );
        #$easy->setopt(Net::Curl::Easy::CURLOPT_URL, $url);
        $easy->setopt( Net::Curl::Easy::CURLOPT_ENCODING, 'gzip,deflate' );
        $easy->setopt( Net::Curl::Easy::CURLOPT_WRITEFUNCTION, sub { my $self = shift; 
                                                                    my $data = shift; 
                                                                    my $len = length($data); 
                                                                    
                                                                    $self->{data} .=  $data;
                                                                    my $url = $self->getinfo( Net::Curl::Easy::CURLINFO_EFFECTIVE_URL );

                                                                return $len; });
#        $easy->setopt(Net::Curl::Easy::CURLOPT_READFUNCTION, sub {});
        $easy->setopt( Net::Curl::Easy::CURLOPT_TIMEOUT, 30 );
        $easy->setopt( Net::Curl::Easy::CURLOPT_NOSIGNAL, 1 );
#        $easy->setopt( Net::Curl::Easy::CURLOPT_FORBID_REUSE, 1 );
        $easy->setopt( Net::Curl::Easy::CURLOPT_SSL_VERIFYPEER, 0 ); #we don't care about ssl, just get the data valid or not

        $easy->setopt( Net::Curl::Easy::CURLOPT_IPRESOLVE, Net::Curl::Easy::CURL_IPRESOLVE_V4 );

        $easy->setopt( Net::Curl::Easy::CURLOPT_DNS_CACHE_TIMEOUT, 36000 ); #timeout dns after ten hours

        push(@{$self->{handles}}, $easy);
    }

    Veelobot::Tools->debug(5, "Done adding handles");

    return 1;
}

#choose next url to harvest based on some factors
sub choose() {
    my $self = shift;

    #my $url = $self->{urls}->extract(int(rand($self->{urls}->pending()-1))); # get random queue item
    my $url = $self->{urls}->dequeue_nb(); # get next queue item

    return undef if ( ! $url ); #return undef if we didn't got any url from queue

    return $url;

    return undef; #this makes no sense, but what the hell ;p
}

sub go() {
    my $self = shift;

    my $running = 1;

    Veelobot::Tools->debug(10, "Go");

    while (1) {

        my $try = 0;
        URLSLOOP: while ( ( (scalar @{$self->{handles}}) > 1) && ($self->{urls}->pending() >= 1) ) {
            Veelobot::Tools->debug(11, "handles - ", scalar @{$self->{handles}}, "running ", $running, "pending", $self->{urls}->pending() );
#            if (my $url = $self->{urls}->dequeue() ) { 
            if (my $url = $self->choose() ) { # get url to harvest
                #check if visited

                if ( $self->feed($url) ) {
                    $running++;
                } else {
                    $self->{urls}->enqueue($url); #push it back, no handles left (for some odd reason!?)
                }

            } else { #we failed to get url to harvest, last
                $try++;
            }

            last URLSLOOP if ( $try >= $self->{urls}->pending() ); #break if we failed to get 100 consequent urls

        }

        #do harvesting
#        my ($r, $w, $e) = $self->{multi}->fdset();
#        my $timeout = $self->{multi}->timeout();
#        select($r, $w, $e, $timeout / 1000) if $timeout > 0;
        $running = $self->{multi}->perform(); #perform curl multi
        while ( my ( $msg, $easy, $result ) = $self->{multi}->info_read() ) { #foreach ready thread
            Veelobot::Tools->debug(7, "cUrl Multi info read: ", $msg, $easy, $result, $easy->getinfo( Net::Curl::Easy::CURLINFO_EFFECTIVE_URL ), $easy->getinfo( Net::Curl::Easy::CURLINFO_RESPONSE_CODE ) );

            my $url = $easy->getinfo( Net::Curl::Easy::CURLINFO_EFFECTIVE_URL ); #just o be shorter...
            Veelobot::Tools->debug(10, "Got ", $url, "content type", $easy->getinfo( Net::Curl::Easy::CURLINFO_CONTENT_TYPE ) );

            #put stuff 
            my %data_hash :shared;
            $data_hash{"url"} = $url;
            $data_hash{"data"} = $easy->{data};
            $self->{data}->enqueue(\%data_hash);

            $easy->{data} = undef;

            $self->{multi}->remove_handle( $easy );
            push(@{$self->{handles}}, $easy);
            
        }
    

    } #main loop


    return 1;

}

sub feed() {
    my $self = shift;
    my $url = shift;
    
    my $easy = undef;

#    return 0 if ! $url;
   
    $easy = pop(@{$self->{handles}});
    if ( ! $easy ) {
        return 0;
    }

    $easy->setopt(Net::Curl::Easy::CURLOPT_URL, $url);
    $self->{multi}->add_handle( $easy );

    Veelobot::Tools->debug(5, "Harvesting ", $url);

    return 1;

}



1;
