#!/usr/bin/perl -w
package Veelobot;

use strict;

use utf8;

#veelobot
use Veelobot::Tools;
use Veelobot::Harvest;

use threads;
use threads::shared;

use Net::Curl::Easy qw(:constants);
use Net::Curl::Easy;

use Compress::LZ4;

use Data::Dumper;


use Try::Tiny;

use HTML::TreeBuilder;

#my $tools = Tools->new();

Veelobot::Tools->debug(10, "Starting");

use Thread::Queue;

use JSON;
use Search::Elasticsearch;

my $urls = Thread::Queue->new();
#my %visited :shared;

my $data = Thread::Queue->new();

#our @urls :shared;

#ignore sig pipe
$SIG{PIPE} = sub { Veelobot::Tools->debug(10, "SIGPIPE received.");  };

sub super_thread() {

    my $num = 0;
    my $size = 0;

    my $e = Search::Elasticsearch->new(
#        trace_to => 'Stdout',
        nodes => [
            '127.0.0.1:9200'
        ]
    );

    while (my $stuff = $data->dequeue()) {
        $num++;
        Veelobot::Tools->debug( 10, "super thread - LEN ", $urls->pending(), "data", $data->pending(), " NUM ", $num, "total size ", $size );
        my $temp = $stuff->{data};

        next if ( ! $temp || length($temp) <= 10 ); # skip if somehow data size is less than 10 bytes
#        $size += length($temp);
#
#
#        dobra kurwa
#        wyciagamy informacje
        my $page = HTML::TreeBuilder->new;
        $page->ignore_unknown(0);
        $page->parse($temp);
        $page->eof();

        my $wpis = $page->look_down('_tag' => 'div', 'data-type' => 'entry');
        next if ! $wpis;

        my $autor = $wpis->look_down('_tag' => 'a', 'class' => 'profile');

        $wpis->look_down('_tag' => 'time')->as_HTML() =~ /datetime="(\S+)"/;
        my $time = $1;

#        print "wyslano $time \r\n";

        $autor->attr('href') =~ /ludzie\/(.*)\//;
        my $user = $1;
        "a" =~ /a/;  # Reset captures to undef.

        my $plus_obj = $wpis->look_down('_tag' => 'p', 'class' => 'vC');
        my $plus = 0;
        $plus = $plus_obj->attr('data-vc') if $plus_obj;

#        print "ilosc plusow $plus \r\n";

        my $sex_obj = $wpis->look_down('_tag' => 'a', 'class' => 'profile')->look_down('_tag' => 'img');
        $sex_obj->as_HTML() =~ /(female|male)/;
        my $sex = $1;
#        print "lalallala $sex \r\n";

        my @tags = $wpis->look_down('_tag' => 'a', 'class' => 'showTagSummary');
        my @tagi = ();
        foreach(@tags) {
#            print " tttttt " . $_->as_text() . "\r\n";
            push(@tagi, $_->as_text());
        }
#        print "aaa $user \r\n";

        my $media = $wpis->look_down('_tag' => 'a', 'target' => '_blank');
        my $link = $media->attr('href') if $media;

#        print "link $link \r\n" if $link;

        my $text = $wpis->look_down('_tag' => 'div', 'class' => 'text')->look_down('_tag' => 'p')->as_text();

#        print "tttteeext $text \r\n";



        #komentarze
        my @comments_obj = $page->look_down('_tag' => 'div', 'data-type' => 'entrycomment');
        my @comments;
        foreach my $comment_obj (@comments_obj) {
            my %com = ();
            my $autorc = $comment_obj->look_down('_tag' => 'a', 'class' => 'profile');
            
            next if ! $autorc; #jesli to nei glowny komentarz to jazda dalej

            $autorc->attr('href') =~ /ludzie\/(.*)\// or next;
            $com{'author'} = $1;
            "a" =~ /a/;  # Reset captures to undef.

            my $plus_objc = $comment_obj->look_down('_tag' => 'p', 'class' => 'vC');
            my $plusc = 0;
            $plusc = $plus_objc->attr('data-vc') if $plus_objc;

            $com{'plus'} = $plusc;

            my $sex_objc = $comment_obj->look_down('_tag' => 'a', 'class' => 'profile')->look_down('_tag' => 'img');
            $sex_objc->as_HTML() =~ /(female|male)/;
            my $sexc = $1;

            $com{'sex'} = $sexc;

            my @tagsc = $comment_obj->look_down('_tag' => 'a', 'class' => 'showTagSummary');
            my @tagic = ();
            foreach(@tagsc) {
                push(@tagic, $_->as_text());
            }
            $com{'tagi'} = ();
            push(@{$com{'tagi'}}, @tagic);

            $com{'text'} = $comment_obj->look_down('_tag' => 'div', 'class' => 'text')->look_down('_tag' => 'p')->as_text();

            $comment_obj->look_down('_tag' => 'time')->as_HTML() =~ /datetime="(\S+)"/;
            $com{'timestamp'} = $1;


            push(@comments, \%com);
        }

        $stuff->{url} =~ /wpis\/(\d+)/;
        my $id = $1;
        "a" =~ /a/;  # Reset captures to undef.

        my %wpis_dict = ();
        $wpis_dict{'author'} = $user;
        $wpis_dict{'text'} = $text;
        $wpis_dict{'timestamp'} = $time;
        $wpis_dict{'tags'} = \@tagi;
        $wpis_dict{'media'} = $link;
        $wpis_dict{'sex'} = $sex;
        $wpis_dict{'plus'} = $plus;
        $wpis_dict{'url'} = $stuff->{url};
        $wpis_dict{'comments'} = \@comments;

#        print Dumper(\%wpis_dict);

#        my $json = encode_json \%wpis_dict;

        $e->index(
            index   =>  'mirko2',
            type    =>  'wpis',
            id      =>  $id,
            body    =>  \%wpis_dict
        );

        #usuwamy drzewko
        $page->delete();
        if ( $urls->pending() == 0 && $data->pending() == 0 ) {
            kill 9, $$;
        }

    }

}

threads->create('super_thread');

#$urls->enqueue("http://www.wykop.pl/wpis/17381507");


my $najnowszy = undef;

if ($ARGV[0] == 0) {
    my $body;
    my $curl = new Net::Curl::Easy;
    $curl->setopt( Net::Curl::Easy::CURLOPT_URL, 'http://wykop.pl/mikroblog');
    $curl->setopt( Net::Curl::Easy::CURLOPT_FOLLOWLOCATION, 1 );
    $curl->setopt( CURLOPT_FILE, \$body );
    $curl->perform;
    my $info = $curl->getinfo(CURLINFO_HTTP_CODE);
    if ($body =~ /data-id="(\d+)"/ ) {
        print "NAJNOWSZY = $1 \r\n";
        $najnowszy = $1;
    } else {
        die('something went really wrong');
    }
}

if ($najnowszy) {
    for (my $i = $najnowszy - 1000; $i <= $najnowszy; $i++) {
        $urls->enqueue("http://www.wykop.pl/wpis/$i/");
    }
} else {
    for (my $i = $ARGV[0]; $i <= $ARGV[1]; $i++) {
        $urls->enqueue("http://www.wykop.pl/wpis/$i/");
    }
}

my $harvest = Veelobot::Harvest->new($urls, $data);

$harvest->add_handles(2);

#threads->create( sub { 
#        my $harvest = Veelobot::Harvest->new($urls, $data);
#        $harvest->add_handles(10);
        $harvest->go(); 
#    } );
$harvest->go();

#super_thread();
Veelobot::Tools->debug(10, "End");
