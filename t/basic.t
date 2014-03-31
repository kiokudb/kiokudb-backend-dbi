#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;

use Test::Requires 'DBD::SQLite';

use KiokuDB::Backend::DBI;
use KiokuDB::Entry;

my $b = KiokuDB::Backend::DBI->new(
    dsn => 'dbi:SQLite:dbname=:memory:',
    columns => [qw(oi)],
);

my $entry = KiokuDB::Entry->new(
    id => "foo",
    root => 1,
    class => "Foo",
    data => { oi => "vey" },
);

my %c = map { $_ => [] } qw(id class data tied root oi);;

$b->entry_to_row($entry, \%c);

is( $c{id}[0], $entry->id, "ID" );

is( $c{class}[0], $entry->class, "class" );

ok( $c{root}[0], "root entry" );

like( $c{data}[0], qr/vey/, "data" );

ok( $c{oi}[0], "extracted column" );

is( $c{oi}[0], "vey", "column data" );

SKIP: {
    skip "SQL::Translator >= 0.11005 is required", 2 unless eval "use SQL::Translator 0.11005";

    $b->deploy;

    $b->txn_do(sub {
        $b->insert( $entry );
    });

    my ( $loaded_entry ) = $b->get("foo");

    isnt( $loaded_entry, $entry, "entries are different" );

    is_deeply( $loaded_entry, $entry, "but eq deeply" );
}

done_testing;
