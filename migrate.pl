#!/usr/bin/perl

use strict;
use warnings;

use Search::Elasticsearch;

my $es   = Search::Elasticsearch->new;

my $bulk = $es->bulk_helper(
    index   => 'mirko2',
    verbose => 1
);
 
$bulk->reindex(
    source  => {
        index   => 'mirko'
    }
);
