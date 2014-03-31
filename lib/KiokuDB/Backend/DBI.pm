package KiokuDB::Backend::DBI;
use Moose;
# ABSTRACT: DBI backend for KiokuDB

use Moose::Util::TypeConstraints;

use MooseX::Types 0.08 -declare => [qw(ValidColumnName SchemaProto)];

use MooseX::Types::Moose qw(ArrayRef HashRef Str Defined);

use Moose::Util::TypeConstraints qw(enum);

use Try::Tiny;
use Data::Stream::Bulk::DBI 0.07;
use SQL::Abstract;
use JSON;
use Scalar::Util qw(weaken refaddr);
use List::MoreUtils qw(any);
use Class::Load qw(load_class);
use Search::GIN 0.07 ();

use KiokuDB 0.46 ();
use KiokuDB::Backend::DBI::Schema;
use KiokuDB::TypeMap;
use KiokuDB::TypeMap::Entry::DBIC::Row;
use KiokuDB::TypeMap::Entry::DBIC::ResultSource;
use KiokuDB::TypeMap::Entry::DBIC::ResultSet;
use KiokuDB::TypeMap::Entry::DBIC::Schema;

use namespace::clean -except => 'meta';

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::Delegate
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::TXN
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple
    KiokuDB::Backend::Role::Query::GIN
    KiokuDB::Backend::Role::Concurrency::POSIX
    KiokuDB::Backend::Role::GC
    Search::GIN::Extract::Delegate
);
# KiokuDB::Backend::Role::TXN::Nested is not supported by many DBs
# we don't really care though

my @std_cols = qw(id class root tied);
my @reserved_cols = ( @std_cols, 'data' );
my %reserved_cols = ( map { $_ => 1 } @reserved_cols );

subtype ValidColumnName, as Str, where { not exists $reserved_cols{$_} };
subtype SchemaProto, as Defined, where {
    load_class($_) unless ref;
    !ref($_) || blessed($_) and $_->isa("DBIx::Class::Schema::KiokuDB");
};

sub new_from_dsn {
    my ( $self, $dsn, @args ) = @_;
    @args = %{ $args[0] } if @args == 1 and ref $args[0] eq 'HASH';
    $self->new( dsn => "dbi:$dsn", @args );
}

sub BUILD {
    my $self = shift;

    $self->schema; # connect early

    if ( $self->create ) {
        $self->create_tables;
    }
}

has '+serializer' => ( default => "json" ); # to make dumps readable

has json => (
    isa => "Object",
    is  => "ro",
    default => sub { JSON->new },
);

has create => (
    isa => "Bool",
    is  => "ro",
    default => 0,
);

has 'dsn' => (
    isa => "Str|CodeRef",
    is  => "ro",
);

has [qw(user password)] => (
    isa => "Str",
    is  => "ro",
);

has dbi_attrs => (
    isa => HashRef,
    is  => "ro",
);

has mysql_strict => (
    isa => "Bool",
    is  => "ro",
    default => 1,
);

has sqlite_sync_mode => (
    isa => enum([qw(0 1 2 OFF NORMAL FULL off normal full)]),
    is  => "ro",
    predicate => "has_sqlite_fsync_mode",
);

has on_connect_call => (
    isa => "ArrayRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build_on_connect_call {
    my $self = shift;

    my @call;

    if ( $self->mysql_strict ) {
        push @call, sub {
            my $storage = shift;

            if ( $storage->can("connect_call_set_strict_mode") ) {
                $storage->connect_call_set_strict_mode;
            }
        };
    };

    if ( $self->has_sqlite_fsync_mode ) {
        push @call, sub {
            my $storage = shift;

            if ( $storage->sqlt_type eq 'SQLite' ) {
                $storage->dbh_do(sub { $_[1]->do("PRAGMA synchronous=" . $self->sqlite_sync_mode) });
            }
        };
    }

    return \@call;
}

has dbic_attrs => (
    isa => "HashRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build_dbic_attrs {
    my $self = shift;

    return {
        on_connect_call => $self->on_connect_call,
    };
}

has connect_info => (
    isa => ArrayRef,
    is  => "ro",
    lazy_build => 1,
);

sub _build_connect_info {
    my $self = shift;

    return [ $self->dsn, $self->user, $self->password, $self->dbi_attrs, $self->dbic_attrs ];
}

has schema => (
    isa => "DBIx::Class::Schema",
    is  => "ro",
    lazy_build => 1,
    init_arg => "connected_schema",
    handles  => [qw(deploy kiokudb_entries_source_name)],
);

has _schema_proto => (
    isa => SchemaProto,
    is  => "ro",
    init_arg => "schema",
    default  => "KiokuDB::Backend::DBI::Schema",
);

has schema_hook => (
    isa => "CodeRef|Str",
    is  => "ro",
    predicate => "has_schema_hook",
);

sub _build_schema {
    my $self = shift;

    my $schema = $self->_schema_proto->clone;

    unless ( $schema->kiokudb_entries_source_name ) {
        $schema->define_kiokudb_schema( extra_entries_columns => $self->columns );
    }

    if ( $self->has_schema_hook ) {
        my $h = $self->schema_hook;
        $self->$h($schema);
    }

    $schema->connect(@{ $self->connect_info });
}

has storage => (
    isa => "DBIx::Class::Storage::DBI",
    is  => "rw",
    lazy_build => 1,
    handles    => [qw(dbh_do)],
);

sub _build_storage { shift->schema->storage }

has for_update => (
    isa => "Bool",
    is  => "ro",
    default => 1,
);

has _for_update => (
    isa => "Bool",
    is  => "ro",
    lazy_build => 1,
);

sub _build__for_update {
    my $self = shift;

    return (
        $self->for_update
            and
        $self->storage->sqlt_type =~ /^(?:MySQL|Oracle|PostgreSQL)$/
    );
}

has columns => (
    isa => ArrayRef[ValidColumnName|HashRef],
    is  => "ro",
    default => sub { [] },
);

has _columns => (
    isa => HashRef,
    is  => "ro",
    lazy_build => 1,
);

sub _build__columns {
    my $self = shift;

    my $rs = $self->schema->source( $self->kiokudb_entries_source_name );

    my @user_cols = grep { not exists $reserved_cols{$_} } $rs->columns;

    return { map { $_ => $rs->column_info($_)->{extract} || undef } @user_cols };
}

has _ordered_columns => (
    isa => "ArrayRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build__ordered_columns {
    my $self = shift;
    return [ @reserved_cols, sort keys %{ $self->_columns } ];
}

has _column_order => (
    isa => "HashRef",
    is  => "ro",
    lazy_build => 1,
);

sub _build__column_order {
    my $self = shift;

    my $cols = $self->_ordered_columns;
    return { map { $cols->[$_] => $_ } 0 .. $#$cols }
}

has '+extract' => (
    required => 0,
);

has sql_abstract => (
    isa => "SQL::Abstract",
    is  => "ro",
    lazy_build => 1,
);

sub _build_sql_abstract {
    my $self = shift;

    SQL::Abstract->new;
}

# use a Maybe so we can force undef in the builder
has batch_size => (
    isa => "Maybe[Int]",
    is  => "ro",
    lazy => 1,
    builder => '_build_batch_size',
);

sub _build_batch_size {
    my $self = shift;

    if ($self->storage->sqlt_type eq 'SQLite') {
        return 999;
    } else {
        return undef;
    }
}

sub has_batch_size { defined shift->batch_size }

sub register_handle {
    my ( $self, $kiokudb ) = @_;

    $self->schema->_kiokudb_handle($kiokudb);
}

sub default_typemap {
    KiokuDB::TypeMap->new(
        isa_entries => {
            # redirect to schema row
            'DBIx::Class::Row'          => KiokuDB::TypeMap::Entry::DBIC::Row->new,

            # actual serialization
            'DBIx::Class::ResultSet'    => KiokuDB::TypeMap::Entry::DBIC::ResultSet->new,

            # fake, the entries never get written to the db
            'DBIx::Class::ResultSource' => KiokuDB::TypeMap::Entry::DBIC::ResultSource->new,
            'DBIx::Class::Schema'       => KiokuDB::TypeMap::Entry::DBIC::Schema->new,
        },
    );
}

sub insert {
    my ( $self, @entries ) = @_;

    return unless @entries;

    my $g = $self->schema->txn_scope_guard;

    $self->insert_rows( $self->entries_to_rows(@entries) );

    # hopefully we're in a transaction, otherwise this totally sucks
    if ( $self->extract ) {
        my %gin_index;

        foreach my $entry ( @entries ) {
            my $id = $entry->id;

            if ( $entry->deleted || !$entry->has_object ) {
                $gin_index{$id} = [];
            } else {
                my $d = $entry->backend_data || $entry->backend_data({});
                $gin_index{$id} = [ $self->extract_values( $entry->object, entry => $entry ) ];
            }
        }

        $self->update_index(\%gin_index);
    }

    $g->commit;
}

sub entries_to_rows {
    my ( $self, @entries ) = @_;

    my ( %insert, %update, @dbic );

    foreach my $t ( \%insert, \%update ) {
        foreach my $col ( @{ $self->_ordered_columns } ) {
            $t->{$col} = [];
        }
    }

    foreach my $entry ( @entries ) {
        my $id = $entry->id;

        if ( $id =~ /^dbic:schema/ ) {
            next;
        } elsif ( $id =~ /^dbic:row:/ ) {
            push @dbic, $entry->data;
        } else {
            my $targ = $entry->prev ? \%update : \%insert;

            my $row = $self->entry_to_row($entry, $targ);
        }
    }

    return \( %insert, %update, @dbic );
}

sub entry_to_row {
    my ( $self, $entry, $collector ) = @_;

    for (qw(id class tied)) {
        push @{ $collector->{$_} }, $entry->$_;
    }

    push @{ $collector->{root} }, $entry->root ? 1 : 0;

    push @{ $collector->{data} }, $self->serialize($entry);

    my $cols = $self->_columns;

    foreach my $column ( keys %$cols ) {
        my $c = $collector->{$column};
        if ( my $extract = $cols->{$column} ) {
            if ( my $obj = $entry->object ) {
                push @$c, $obj->$extract($column);
                next;
            }
        } elsif ( ref( my $data = $entry->data ) eq 'HASH' ) {
            if ( exists $data->{$column} and not ref( my $value = $data->{$column} ) ) {
                push @$c, $value;
                next;
            }
        }

        push @$c, undef;
    }
}

sub insert_rows {
    my ( $self, $insert, $update, $dbic ) = @_;

    my $g = $self->schema->txn_scope_guard;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        if ( $self->extract ) {
            if ( my @ids = map { @{ $_->{id} || [] } } $insert, $update ) {

                my $batch_size = $self->batch_size || scalar(@ids);

                my @ids_copy = @ids;
                while ( my @batch_ids = splice @ids_copy, 0, $batch_size ) {
                    my $del_gin_sth = $dbh->prepare_cached("DELETE FROM gin_index WHERE id IN (" . join(", ", ('?') x @batch_ids) . ")");

                    $del_gin_sth->execute(@batch_ids);

                    $del_gin_sth->finish;
                }
            }
        }

        my $colinfo = $self->schema->source('entries')->columns_info;

        my %rows = ( insert => $insert, update => $update );

        foreach my $op (qw(insert update)) {
            my $prepare = "prepare_$op";
            my ( $sth, @cols ) = $self->$prepare($dbh);

            my $i = 1;

            foreach my $column_name (@cols) {
                my $attributes = {};

                if ( exists $colinfo->{$column_name} ) {
                    my $dt = $colinfo->{$column_name}{data_type};
                    $attributes = $self->storage->bind_attribute_by_data_type($dt);
                }

                $sth->bind_param_array( $i, $rows{$op}->{$column_name}, $attributes );

                $i++;
            }

            $sth->execute_array({ArrayTupleStatus => []}) or die;

            $sth->finish;
        }

        $_->insert_or_update for @$dbic;
    });

    $g->commit;
}

sub prepare_select {
    my ( $self, $dbh, $stmt ) = @_;

    $dbh->prepare_cached($stmt . ( $self->_for_update ? " FOR UPDATE" : "" ), {}, 3); # 3 = don't use if still Active
}

sub prepare_insert {
    my ( $self, $dbh ) = @_;

    my @cols = @{ $self->_ordered_columns };

    my $ins = $dbh->prepare_cached("INSERT INTO entries (" . join(", ", @cols) . ") VALUES (" . join(", ", ('?') x @cols) . ")");

    return ( $ins, @cols );
}

sub prepare_update {
    my ( $self, $dbh ) = @_;

    my ( $id, @cols ) = @{ $self->_ordered_columns };

    my $upd = $dbh->prepare_cached("UPDATE entries SET " . join(", ", map { "$_ = ?" } @cols) . " WHERE $id = ?");

    return ( $upd, @cols, $id );
}

sub update_index {
    my ( $self, $entries ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $i_sth = $dbh->prepare_cached("INSERT INTO gin_index (id, value) VALUES (?, ?)");

        foreach my $id ( keys %$entries ) {
            my $rv = $i_sth->execute_array(
                {ArrayTupleStatus => []},
                $id,
                $entries->{$id},
            );
        }

        $i_sth->finish;
    });
}

sub _parse_dbic_key {
    my ( $self, $key ) = @_;

    @{ $self->json->decode(substr($key,length('dbic:row:'))) };
}

sub _part_rows_and_ids {
    my ( $self, $rows_and_ids ) = @_;

    my ( @rows, @ids, @special );

    for ( @$rows_and_ids ) {
        if ( /^dbic:schema/ ) {
            push @special, $_;
        } elsif ( /^dbic:row:/ ) {
            push @rows, $_;
        } else {
            push @ids, $_;
        }
    }

    return \( @rows, @ids, @special );
}

sub _group_dbic_keys {
    my ( $self, $keys, $mkey_handler ) = @_;

    my ( %keys, %ids );

    foreach my $id ( @$keys ) {
        my ( $rs_name, @key ) = $self->_parse_dbic_key($id);

        if ( @key > 1 ) {
            $mkey_handler->($id, $rs_name, @key);
        } else {
            # for other objects we queue up IDs for a single SELECT
            push @{ $keys{$rs_name} ||= [] }, $key[0];
            push @{ $ids{$rs_name}  ||= [] }, $id;
        }
    }

    return \( %keys, %ids );
}

sub get {
    my ( $self, @rows_and_ids ) = @_;

    return unless @rows_and_ids;

    my %entries;

    my ( $rows, $ids, $special ) = $self->_part_rows_and_ids(\@rows_and_ids);

    if ( @$ids ) {
        $self->dbh_do(sub {
            my ( $storage, $dbh ) = @_;

            my @ids_copy = @$ids;

            my $batch_size = $self->batch_size || scalar(@$ids);

            while ( my @batch_ids = splice(@ids_copy, 0, $batch_size) ) {
                my $sth = $self->prepare_select($dbh, "SELECT id, data FROM entries WHERE id IN (" . join(", ", ('?') x @batch_ids) . ")");
                $sth->execute(@batch_ids);

                $sth->bind_columns( \my ( $id, $data ) );

                # not actually necessary but i'm keeping it around for reference:
                #my ( $id, $data );
                #use DBD::Pg qw(PG_BYTEA);
                #$sth->bind_col(1, \$id);
                #$sth->bind_col(2, \$data, { pg_type => PG_BYTEA });

                while ( $sth->fetch ) {
                    $entries{$id} = $data;
                }
            }
        });
    }

    if ( @$rows ) {
        my $schema = $self->schema;

        my $err = \"foo";
        my ( $rs_keys, $rs_ids ) = try {
            $self->_group_dbic_keys( $rows, sub {
                my ( $id, $rs_name, @key ) = @_;

                # multi column primary keys need 'find'
                my $obj = $schema->resultset($rs_name)->find(@key) or die $err; # die to stop search

                $entries{$id} = KiokuDB::Entry->new(
                    id    => $id,
                    class => ref($obj),
                    data  => $obj,
                );
            });
        } catch {
            die $_ if ref $_ and refaddr($_) == refaddr($err);
        } or return;

        foreach my $rs_name ( keys %$rs_keys ) {
            my $rs = $schema->resultset($rs_name);

            my $ids = $rs_ids->{$rs_name};

            my @objs;

            if ( @$ids == 1 ) {
                my $id = $ids->[0];

                my $obj = $rs->find($rs_keys->{$rs_name}[0]) or return;

                $entries{$id} = KiokuDB::Entry->new(
                    id => $id,
                    class => ref($obj),
                    data => $obj,
                );
            } else {
                my ($pk) = $rs->result_source->primary_columns;

                my $keys = $rs_keys->{$rs_name};

                my @objs = $rs->search({ $pk => $keys })->all;

                return if @objs != @$ids;

                # this key lookup is because it's not returned in the same order
                my %pk_to_id;
                @pk_to_id{@$keys} = @$ids;

                foreach my $obj ( @objs ) {
                    my $id = $pk_to_id{$obj->id};
                    $entries{$id} = KiokuDB::Entry->new(
                        id    => $id,
                        class => ref($obj),
                        data  => $obj,
                    );
                }
            }
        }
    }

    for ( @$special ) {
        $entries{$_} = KiokuDB::Entry->new(
            id => $_,
            $_ eq 'dbic:schema'
                ? ( data => $self->schema,
                    class => "DBIx::Class::Schema" )
                : ( data => undef,
                    class => "DBIx::Class::ResultSource" )
        );
    }

    # ->rows only works after we're done
    return if @rows_and_ids != keys %entries;
    # case sensitivity differences, possibly?
    return if any { !defined } @entries{@rows_and_ids};

    map { ref($_) ? $_ : $self->deserialize($_) } @entries{@rows_and_ids};
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    # FIXME special DBIC rows

    my @ids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $g = $self->schema->txn_scope_guard;

        my $batch_size = $self->batch_size || scalar(@ids);

        my @ids_copy = @ids;
        while ( my @batch_ids = splice @ids_copy, 0, $batch_size ) {
            if ( $self->extract ) {
                # FIXME rely on cascade delete?
                my $sth = $dbh->prepare_cached("DELETE FROM gin_index WHERE id IN (" . join(", ", ('?') x @batch_ids) . ")");
                $sth->execute(@batch_ids);
                $sth->finish;
            }

            my $sth = $dbh->prepare_cached("DELETE FROM entries WHERE id IN (" . join(", ", ('?') x @batch_ids) . ")");
            $sth->execute(@batch_ids);
            $sth->finish;
        }

        $g->commit;
    });

    return;
}

sub exists {
    my ( $self, @rows_and_ids ) = @_;

    return unless @rows_and_ids;

    my $schema = $self->schema;

    my %entries;

    my ( $rows, $ids, $special ) = $self->_part_rows_and_ids(\@rows_and_ids);

    if ( @$ids ) {
        $self->dbh_do(sub {
            my ( $storage, $dbh ) = @_;

            my $batch_size = $self->batch_size || scalar(@$ids);

            my @ids_copy = @$ids;
            while ( my @batch_ids = splice @ids_copy, 0, $batch_size ) {
                my $sth = $self-> prepare_select ( $dbh, "SELECT id FROM entries WHERE id IN (" . join(", ", ('?') x @batch_ids) . ")");
                $sth->execute(@batch_ids);

                $sth->bind_columns( \( my $id ) );

                $entries{$id} = 1 while $sth->fetch;
            }
        });
    }

    if ( @$rows ) {
        my ( $rs_keys, $rs_ids ) = $self->_group_dbic_keys( $rows, sub {
            my ( $id, $rs_name, @key ) = @_;
            $entries{$id} = defined $schema->resultset($rs_name)->find(@key); # FIXME slow
        });

        foreach my $rs_name ( keys %$rs_keys ) {
            my $rs = $schema->resultset($rs_name);

            my $ids = $rs_ids->{$rs_name};
            my $keys = $rs_keys->{$rs_name};

            my ( $pk ) = $rs->result_source->primary_columns;

            my @exists = $rs->search({ $pk => $keys })->get_column($pk)->all;

            my %pk_to_id;
            @pk_to_id{@$keys} = @$ids;

            @entries{@pk_to_id{@exists}} = ( (1) x @exists );
        }
    }

    for ( @$special ) {
        if ( $_ eq 'dbic:schema' ) {
            $entries{$_} = 1;
        } elsif ( /^dbic:schema:(.*)/ ) {
            $entries{$_} = defined try { $schema->source($1) };
        }
    }

    return @entries{@rows_and_ids};
}

sub txn_begin    { shift->storage->txn_begin(@_) }
sub txn_commit   { shift->storage->txn_commit(@_) }
sub txn_rollback { shift->storage->txn_rollback(@_) }

sub clear {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        $dbh->do("DELETE FROM gin_index");
        $dbh->do("DELETE FROM entries");
    });
}

sub _sth_stream {
    my ( $self, $sql, @bind ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;
        my $sth = $self->prepare_select($dbh, $sql);

        $sth->execute(@bind);

        Data::Stream::Bulk::DBI->new( sth => $sth );
    });
}

sub _select_entry_stream {
    my ( $self, @args ) = @_;

    my $stream = $self->_sth_stream(@args);

    return $stream->filter(sub { [ map { $self->deserialize($_->[0]) } @$_ ] });
}

sub all_entries {
    my $self = shift;
    $self->_select_entry_stream("SELECT data FROM entries");
}

sub root_entries {
    my $self = shift;
    $self->_select_entry_stream("SELECT data FROM entries WHERE root");
}

sub child_entries {
    my $self = shift;
    $self->_select_entry_stream("SELECT data FROM entries WHERE not root");
}

sub _select_id_stream {
    my ( $self, @args ) = @_;

    my $stream = $self->_sth_stream(@args);

    return $stream->filter(sub {[ map { $_->[0] } @$_ ]});
}

sub all_entry_ids {
    my $self = shift;
    $self->_select_id_stream("SELECT id FROM entries");
}

sub root_entry_ids {
    my $self = shift;
    $self->_select_id_stream("SELECT id FROM entries WHERE root");
}

sub child_entry_ids {
    my $self = shift;
    $self->_select_id_stream("SELECT id FROM entries WHERE not root");
}

sub simple_search {
    my ( $self, $proto ) = @_;

    my ( $where_clause, @bind ) = $self->sql_abstract->where($proto);

    $self->_select_entry_stream("SELECT data FROM entries $where_clause", @bind);
}

sub search {
    my ( $self, $query, @args ) = @_;

    my %args = (
        distinct => $self->distinct,
        @args,
    );

    my %spec = $query->extract_values($self);
    my @binds;

    my $inner_sql = $self->_search_gin_subquery(\%spec, \@binds);
    return $self->_select_entry_stream("SELECT data FROM entries WHERE id IN (".$inner_sql.")",@binds);
}

sub _search_gin_subquery {
    my ($self, $spec, $binds) = @_;

    my @v = ref $spec->{values} eq 'ARRAY' ? @{ $spec->{values} } : ();
    if ( $spec->{method} eq 'set' ) {
        my $op = $spec->{operation};

        die 'gin set query received bad operation'
          unless $op =~ /^(UNION|INTERSECT|EXCEPT)$/i;

        die 'gin set query missing subqueries'
          unless ref $spec->{subqueries} eq 'ARRAY' &&
            scalar @{ $spec->{subqueries} };

        return "(".
          (
           join ' '.$op.' ',
           map { $self->_search_gin_subquery($_, $binds) }
           @{ $spec->{subqueries} }
          ).")";

    } elsif ( $spec->{method} eq 'all' and @v > 1) {
        # for some reason count(id) = ? doesn't work
        push @$binds, @v;
        return "SELECT id FROM gin_index WHERE value IN ".
          "(" . join(", ", ('?') x @v) . ")" .
            "GROUP BY id HAVING COUNT(id) = " . scalar(@v);
    } else {
        push @$binds, @v;
        return "SELECT DISTINCT id FROM gin_index WHERE value IN ".
          "(" . join(", ", ('?') x @v) . ")";
    }
}

sub fetch_entry { die "TODO" }

sub remove_ids {
    my ( $self, @ids ) = @_;

    die "Deletion the GIN index is handled implicitly";
}

sub insert_entry {
    my ( $self, $id, @keys ) = @_;

    die "Insertion to the GIN index is handled implicitly";
}

sub _table_info {
    my ( $self, $catalog, $schema, $table ) = @_;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        my $filter = ( $self->storage->sqlt_type eq 'SQLite' ? '%' : '' );

        foreach my $arg ( $catalog, $schema, $table ) {
            $arg = $filter unless defined $arg;
        }

        $dbh->table_info($catalog, $schema, $table, 'TABLE')->fetchall_arrayref;
    });
}

sub tables_exist {
    my $self = shift;

    return ( @{ $self->_table_info(undef, undef, 'entries') } > 0 );
}

sub create_tables {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        unless ( $self->tables_exist ) {
            $self->deploy({ producer_args => { mysql_version => 4.1 } });
        }
    });
}

sub drop_tables {
    my $self = shift;

    $self->dbh_do(sub {
        my ( $storage, $dbh ) = @_;

        $dbh->do("DROP TABLE gin_index");
        $dbh->do("DROP TABLE entries");
    });
}

sub DEMOLISH {
    my $self = shift;
    return if $_[0];

    if ( $self->has_storage ) {
        $self->storage->disconnect;
    }
}

sub new_garbage_collector {
    my ( $self, %args ) = @_;

    if ( grep { $_ !~ /^(?:entries|gin_index)/ } map { $_->[2] } @{ $self->_table_info } ) {
        die "\nRefusing to GC a database with additional tables.\n\nThis is ecause the root set and referencing scheme might be ambiguous (it's not yet clear what garbage collection should actually do on a mixed schema).\n";
    } else {
        my $cmd = $args{command};
        my $class = $args{class} || $cmd ? $cmd->class : "KiokuDB::GC::Naive";

        load_class($class);

        return $class->new(
            %args,
            backend => $self,
            ( $cmd ? ( verbose => $cmd->verbose ) : $cmd ),
        );
    }
}

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

=pod

=head1 SYNOPSIS

    my $dir = KiokuDB->connect(
        "dbi:mysql:foo",
        user     => "blah",
        password => "moo',
        columns  => [
            # specify extra columns for the 'entries' table
            # in the same format you pass to DBIC's add_columns

            name => {
                data_type => "varchar",
                is_nullable => 1, # probably important
            },
        ],
    );

    $dir->search({ name => "foo" }); # SQL::Abstract

=head1 DESCRIPTION

This backend for L<KiokuDB> leverages existing L<DBI> accessible databases.

The schema is based on two tables, C<entries> and C<gin_index> (the latter is
only used if a L<Search::GIN> extractor is specified).

The C<entries> table has two main columns, C<id> and C<data> (currently in
JSPON format, in the future the format will be pluggable), and additional user
specified columns.

The user specified columns are extracted from inserted objects using a callback
(or just copied for simple scalars), allowing SQL where clauses to be used for
searching.

=head1 COLUMN EXTRACTIONS

The columns are specified using a L<DBIx::Class::ResultSource> instance.

One additional column info parameter is used, C<extract>, which is called as a
method on the inserted object with the column name as the only argument. The
return value from this callback will be used to populate the column.

If the column extractor is omitted then the column will contain a copy of the
entry data key by the same name, if it is a plain scalar. Otherwise the column
will be C<NULL>.

These columns are only used for lookup purposes, only C<data> is consulted when
loading entries.

=head1 DBIC INTEGRATION

This backend is layered on top of L<DBIx::Class::Storage::DBI> and reused
L<DBIx::Class::Schema> for DDL.

Because of this objects from a L<DBIx::Class::Schema> can refer to objects in
the KiokuDB entries table, and vice versa.

For more details see L<DBIx::Class::Schema::KiokuDB>.

=head1 SUPPORTED DATABASES

This driver has been tested with MySQL 5 (4.1 should be the minimal supported
version), SQLite 3, and PostgreSQL 8.3.

The SQL code is reasonably portable and should work with most databases. Binary
column support is required when using the L<Storable> serializer.

=head2 Transactions

For reasons of performance and ease of use database vendors ship with read
committed transaction isolation by default.

This means that read locks are B<not> acquired when data is fetched from the
database, allowing it to be updated by another writer. If the current
transaction then updates the value it will be silently overwritten.

IMHO this is a much bigger problem when the data is unstructured. This is
because data is loaded and fetched in potentially smaller chunks, increasing
the risk of phantom reads.

Unfortunately enabling truly isolated transaction semantics means that
C<txn_commit> may fail due to a lock contention, forcing you to repeat your
transaction. Arguably this is more correct "read comitted", which can lead to
race conditions.

Enabling repeatable read or serializable transaction isolation prevents
transactions from interfering with eachother, by ensuring all data reads are
performed with a shared lock.

For more information on isolation see
L<http://en.wikipedia.org/wiki/Isolation_(computer_science)>

=head3 SQLite

SQLite provides serializable isolation by default.

L<http://www.sqlite.org/pragma.html#pragma_read_uncommitted>

=head3 MySQL

MySQL provides read committed isolation by default.

Serializable level isolation can be enabled by by default by changing the
C<transaction-isolation> global variable,

L<http://dev.mysql.com/doc/refman/5.1/en/set-transaction.html#isolevel_serializable>

=head3 PostgreSQL

PostgreSQL provides read committed isolation by default.

Repeatable read or serializable isolation can be enabled by setting the default
transaction isolation level, or using the C<SET TRANSACTION> SQL statement.

L<http://www.postgresql.org/docs/8.3/interactive/transaction-iso.html>,
L<http://www.postgresql.org/docs/8.3/interactive/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION>

=head1 ATTRIBUTES

=over 4

=item schema

Created automatically.

This is L<DBIx::Class::Schema> object that is used for schema deployment,
connectivity and transaction handling.

=item connect_info

An array reference whose contents are passed to L<DBIx::Class::Schema/connect>.

If omitted will be created from the attrs C<dsn>, C<user>, C<password> and
C<dbi_attrs>.

=item dsn

=item user

=item password

=item dbi_attrs

Convenience attrs for connecting using L<KiokuDB/connect>.

User in C<connect_info>'s builder.

=item columns

Additional columns, see L</"COLUMN EXTRACTIONS">.

=item serializer

L<KiokuDB::Serializer>. Coerces from a string, too:

    KiokuDB->connect("dbi:...", serializer => "storable");

Defaults to L<KiokuDB::Serializer::JSON>.

=item create

If true the existence of the tables will be checked for and the DB will be
deployed if not.

Defaults to false.

=item extract

An optional L<Search::GIN::Extract> used to create the C<gin_index> entries.

Usually L<Search::GIN::Extract::Callback>.

=item schema_hook

A hook that is called on the backend object as a method with the schema as the
argument just before connecting.

If you need to modify the schema in some way (adding indexes or constraints)
this is where it should be done.

=item for_update

If true (the defaults), will cause all select statement to be issued with a
C<FOR UPDATE> modifier on MySQL, Postgres and Oracle.

This is highly reccomended because these database provide low isolation
guarantees as configured out the box, and highly interlinked graph databases
are much more susceptible to corruption because of lack of transcational
isolation than normalized relational databases.

=item sqlite_sync_mode

If this attribute is set and the underlying database is SQLite, then
C<PRAGMA syncrhonous=...> will be issued with this value.

Can be C<OFF>, C<NORMAL> or C<FULL> (SQLite's default), or 0, 1, or 2.

See L<http://www.sqlite.org/pragma.html#pragma_synchronous>.

=item mysql_strict

If true (the default), sets MySQL's strict mode.

This is B<HIGHLY> reccomended, or you may enjoy some of MySQL's more
interesting features, like automatic data loss when the columns are too narrow.

See L<http://dev.mysql.com/doc/refman/5.0/en/server-sql-mode.html> and
L<DBIx::Class::Storage::DBI::mysql> for more details.

=item on_connect_call

See L<DBIx::Class::Storage::DBI>.

This attribute is constructed based on the values of C<mysql_version> and
C<sqlite_sync_mode>, but may be overridden if you need more control.

=item dbic_attrs

See L<DBIx::Class::Storage::DBI>.

Defaults to

    { on_connect_call => $self->on_connect_call }

=item batch_size

SQL that deals with entries run in batches of the amount provided in
C<batch_size>. If it is not provided, the statements will run in a single
batch.

This solves the issue with SQLite where lists can only handle 999
elements at a time. C<batch_size> will be set to 999 by default if the
driver in use is SQLite.

=back

=head1 METHODS

See L<KiokuDB::Backend> and the various roles for more info.

=over 4

=item deploy

Calls L<DBIx::Class::Schema/deploy>.

Deployment to MySQL requires that you specify something like:

    $dir->backend->deploy({ producer_args => { mysql_version => 4 } });

because MySQL versions before 4 did not have support for boolean types, and the
schema emitted by L<SQL::Translator> will not work with the queries used.

=item drop_tables

Drops the C<entries> and C<gin_index> tables.

=back

=head1 TROUBLESHOOTING

=head2 I get C<unexpected end of string while parsing JSON string>

You are problably using MySQL, which comes with a helpful data compression
feature: when your serialized objects are larger than the maximum size of a
C<BLOB> column MySQL will simply shorten it for you.

Why C<BLOB> defaults to 64k, and how on earth someone would consider silent
data truncation a sane default I could never fathom, but nevertheless MySQL
does allow you to disable this by setting the "strict" SQL mode in the
configuration.

To resolve the actual problem (though this obviously won't repair your lost
data), alter the entries table so that the C<data> column uses the nonstandard
C<LONGBLOB> datatype.

=head1 VERSION CONTROL

KiokuDB-Backend-DBI is maintained using Git. Information about the repository
is available on L<http://www.iinteractive.com/kiokudb/>

=begin Pod::Coverage

BUILD
DEMOLISH
all_entries
all_entry_ids
child_entries
child_entry_ids
clear
create_tables
default_typemap
delete
entries_to_rows
entry_to_row
exists
fetch_entry
has_batch_size
insert
insert_entry
insert_rows
new_from_dsn
new_garbage_collector
prepare_insert
prepare_select
prepare_update
register_handle
remove_ids
root_entries
root_entry_ids
search
simple_search
tables_exist
txn_begin
txn_commit
txn_rollback
update_index

=end Pod::Coverage

=cut
