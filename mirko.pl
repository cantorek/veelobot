#!/usr/bin/perl -w
package Veelobot;


use strict;

#veelobot
use Veelobot::Tools;
use Veelobot::Harvest;

use threads;
use threads::shared;

use Compress::LZ4;

use Data::Dumper;


use Try::Tiny;

#my $tools = Tools->new();

Veelobot::Tools->debug(10, "Starting");

use Thread::Queue;

my $urls = Thread::Queue->new();
#my %visited :shared;

my $data = Thread::Queue->new();

#our @urls :shared;

#ignore sig pipe
$SIG{PIPE} = sub { Veelobot::Tools->debug(10, "SIGPIPE received.");  };

sub super_thread() {
    my $next = 0;
    open(PLIK, ">:raw", "dupa.db.$next");

    my $num = 0;
    my $size = 0;

    while (my $stuff = $data->dequeue()) {
        $num++;
        Veelobot::Tools->debug( 10, "super thread - LEN ", $urls->pending(), "data", $data->pending(), " NUM ", $num, "total size ", $size );
        my $temp = $stuff->{data};
        if ( $size >= 1073741824 ) { #rotate @ 1gb
            close(PLIK);
            open(PLIK, ">:raw", "dupa.db.$next");
            $size = 0;
            $next++;
        }
        next if ( ! $temp || length($temp) <= 10 ); # skip if somehow data size is less than 10 bytes
        $size += length($temp);
        my $compressed = Compress::LZ4::compress($temp);
        my $url_len = length($stuff->{url});
        my $data_len = length($compressed);
        my $p = pack("II", $url_len, $data_len);
        Veelobot::Tools->debug( 10, "url_len", $url_len, "compressed Data len", $data_len , "size", $size);
        print PLIK $p; #write header to file
        print PLIK $stuff->{url}; #write url
        print PLIK $compressed; #write compressed webpage
    }
    close(PLIK);

}

threads->create('super_thread');

for (my $i = 1; $i < 100000; $i++) {
    $urls->enqueue("http://www.wykop.pl/wpis/$i/");
}

my $harvest = Veelobot::Harvest->new($urls, $data);

$harvest->add_handles(100);

$harvest->go();

Veelobot::Tools->debug(10, "End");
