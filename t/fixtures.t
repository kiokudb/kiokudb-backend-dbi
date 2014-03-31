#!/usr/bin/perl

use Test::More;

BEGIN {
    plan skip_all => "DBD::SQLite and SQL::Translator >= 0.11005 are required"
        unless eval "use DBI; use DBD::SQLite; use DBIx::Class::Optional::Dependencies; 1";

    plan skip_all => DBIx::Class::Optional::Dependencies->req_missing_for("deploy")
        unless DBIx::Class::Optional::Dependencies->req_ok_for("deploy");
}

use Test::TempDir;

use KiokuDB;
use KiokuDB::Backend::DBI;

use KiokuDB::Test;

use Search::GIN::Extract::Class;

# real file needed for concurrency tests
my $sqlite = "dbi:SQLite:dbname=" . temp_root->file("db");

foreach my $dsn (
    [ $sqlite ],
    #[ "dbi:mysql:test" ],
    #[ "dbi:Pg:dbname=test" ],
) {
    foreach my $serializer (qw(json storable), eval { require YAML::XS; "yaml" }) {
        #diag "testing against $dsn->[0] with $serializer\n";

        my $connect = sub {
            KiokuDB->connect(
                @$dsn,
                create => 1,
                serializer => $serializer,
                columns => [
                    name => {
                        is_nullable => 1,
                        data_type   => "varchar",
                    },
                    age => {
                        is_nullable => 1,
                        data_type   => "integer",
                    },
                ],
                extract => Search::GIN::Extract::Class->new,
                sqlite_sync_mode => 'OFF',
            );
        };

        {
            my $dir = $connect->();
            $dir->txn_do(sub { $dir->backend->clear });
        }

        run_all_fixtures($connect);

        if ( grep { !$_ } Test::Builder->new->summary ) {
            diag "Leaving tables in $dsn->[0] due to test failures";
        } else {
            $connect->()->backend->drop_tables;
        }
    }
}

done_testing;
