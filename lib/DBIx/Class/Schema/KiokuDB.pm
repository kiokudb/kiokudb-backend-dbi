package DBIx::Class::Schema::KiokuDB;
# ABSTRACT: Hybrid KiokuDB/DBIx::Class::Schema schema support.

use strict;
use warnings;

use Carp qw(croak);

use DBI 1.607 ();
use DBIx::Class 0.08127 ();
use DBIx::Class::KiokuDB::EntryProxy;
use DBIx::Class::ResultSource::Table;

use Scalar::Util qw(weaken refaddr);

use namespace::clean;

use base qw(Class::Accessor::Grouped);

__PACKAGE__->mk_group_accessors( inherited => "kiokudb_entries_source_name" );

sub kiokudb_handle {
    my $self = shift;

    croak "Can't call kiokudb_handle on unconnected schema" unless ref $self;

    unless ( $self->{kiokudb_handle} ) {
        require KiokuDB;
        require KiokuDB::Backend::DBI;

        croak "Can't vivify KiokuDB handle without KiokuDB schema bits. " .
              "Add __PACKAGE__->define_kiokudb_schema() to your schema class"
                  unless $self->kiokudb_entries_source_name;

        my $dir = KiokuDB->new(
            backend => my $backend = KiokuDB::Backend::DBI->new(
                connected_schema => $self,
            ),
        );

        $backend->meta->get_attribute('schema')->_weaken_value($backend); # FIXME proper MOP api?

        # not weak
        $self->{kiokudb_handle} = $dir;
    }

    $self->{kiokudb_handle};
}

sub _kiokudb_handle {
    my ( $self, $handle ) = @_;

    croak "Can't call _kiokudb_handle on unconnected schema" unless ref $self;

    croak "Can't vivify KiokuDB handle without KiokuDB schema bits. " .
          "Add __PACKAGE__->define_kiokudb_schema() to your schema class"
              unless $self->kiokudb_entries_source_name;

    if ( $self->{kiokudb_handle} ) {
        if ( refaddr($self->{kiokudb_handle}) != refaddr($handle) ) {
            croak "KiokuDB directory already registered";
        }
    } else {
        $self->{kiokudb_handle} = $handle;
        weaken($self->{kiokudb_handle});
    }

    return $handle;
}

sub define_kiokudb_schema {
    my ( $self, @args ) = @_;

    my %args = (
        schema          => $self,
        entries_table   => "entries",
        gin_index_table => "gin_index",
        result_class    => "DBIx::Class::KiokuDB::EntryProxy",
        gin_index       => 1,
        @args,
    );

    my $entries_source_name   = $args{entries_source}   ||= $args{entries_table};
    my $gin_index_source_name = $args{gin_index_source} ||= $args{gin_index_table};

    my $entries = $self->define_kiokudb_entries_resultsource(%args);

    my $schema = $args{schema};

    $schema->register_source( $entries_source_name   => $entries );
    if ($args{gin_index}) {
        my $gin_index = $self->define_kiokudb_gin_index_resultsource(%args);
        $schema->register_source( $gin_index_source_name => $gin_index );
    }


    $schema->kiokudb_entries_source_name($entries_source_name)
        unless $schema->kiokudb_entries_source_name;
}

sub define_kiokudb_entries_resultsource {
    my ( $self, %args ) = @_;

    my $entries = DBIx::Class::ResultSource::Table->new({ name => $args{entries_table} });

    $entries->add_columns(
        id    => { data_type => "varchar" },
        data  => { data_type => "blob", is_nullable => 0 }, # FIXME longblob for mysql
        class => { data_type => "varchar", is_nullable => 1 },
        root  => { data_type => "boolean", is_nullable => 0 },
        tied  => { data_type => "char", size => 1, is_nullable => 1 },
        @{ $args{extra_entries_columns} || [] },
    );

    $entries->set_primary_key("id");

    $entries->sqlt_deploy_callback(sub {
        my ($source, $sqlt_table) = @_;

        $sqlt_table->extra->{mysql_table_type} = "InnoDB";

        if ( $source->schema->storage->sqlt_type eq 'MySQL' ) {
            $sqlt_table->get_field('data')->data_type('longblob');
        }
    });

    $entries->result_class($args{result_class});

    return $entries;
}

sub define_kiokudb_gin_index_resultsource {
    my ( $self, %args ) = @_;

    my $gin_index = DBIx::Class::ResultSource::Table->new({ name => $args{gin_index_table} });

    $gin_index->add_columns(
        id    => { data_type => "varchar", is_foreign_key => 1 },
        value => { data_type => "varchar" },
    );

    $gin_index->add_relationship('entry_ids', $args{entries_source}, { 'foreign.id' => 'self.id' });

    $gin_index->sqlt_deploy_callback(sub {
        my ($source, $sqlt_table) = @_;


        $sqlt_table->extra->{mysql_table_type} = "InnoDB";

        $sqlt_table->add_index( name => 'gin_index_ids', fields => ['id'] )
            or die $sqlt_table->error;

        $sqlt_table->add_index( name => 'gin_index_values', fields => ['value'] )
            or die $sqlt_table->error;
    });

    return $gin_index;
}

# ex: set sw=4 et:

__PACKAGE__

__END__

=pod

=head1 SYNOPSIS

Load this component into the schema:

    package MyApp::DB;
    use base qw(DBIx::Class::Schema);

    __PACKAGE__->load_components(qw(Schema::KiokuDB));

    __PAKCAGE__->load_namespaces;

Then load the L<DBIx::Class::KiokuDB> component into every table that wants to
refer to arbitrary KiokuDB objects:

    package MyApp::DB::Result::Album;
    use base qw(DBIx::Class::Core);

    __PACKAGE__->load_components(qw(KiokuDB));

    __PACKAGE__->table('album');

    __PACKAGE__->add_columns(
        id => { data_type => "integer" },
        title => { data_type => "varchar" },

        # the foreign key for the KiokuDB object:
        metadata => { data_type => "varchar" },
    );

    __PACKAGE__->set_primary_key('id');

    # enable a KiokuDB rel on the column:
    __PACKAGE__->kiokudb_column('metadata');

Connect to the DSN:

    my $dir = KiokuDB->connect(
        'dbi:SQLite:dbname=:memory:',
        schema => "MyApp::DB",
        create => 1,
    );

    # get the connect DBIC schema instance
    my $schema = $dir->backend->schema;

Then you can freely refer to KiokuDB objects from your C<Album> class:

    $dir->txn_do(scope => 1, body => sub {

        $schema->resultset("Album")->create({
            title => "Blah blah",
            metadata => $any_object,
        });
    });

=head1 DESCRIPTION

This class provides the schema definition support code required for integrating
an arbitrary L<DBIx::Class::Schema> with L<KiokuDB::Backend::DBI>.

=head2 REUSING AN EXISTING DBIx::Class SCHEMA

The example in the Synopis assumes that you want to first set up a
L<KiokuDB> and than link that to some L<DBIx::Class> classes. Another
use case is that you already have a configured L<DBIx::Class> Schema
and want to tack L<KiokuDB> onto it.

The trick here is to make sure to load the L<KiokuDB> schema using
C<< __PACKAGE__->define_kiokudb_schema() >> in your Schema class:

    package MyApp::DB;
    use base qw(DBIx::Class::Schema);

    __PACKAGE__->load_components(qw(Schema::KiokuDB));
    __PACKAGE__->define_kiokudb_schema();

    __PAKCAGE__->load_namespaces;

You can now get the L<KiokuDB> directory handle like so:

    my $dir = $schema->kiokudb_handle;

For a complete example take a look at F<t/autovivify_handle.t>.

=head1 USAGE AND LIMITATIONS

L<KiokuDB> managed objects may hold references to row objects, resultsets
(treated as saved searches, or results or cursor state is saved), result source
handles, and the schema.

Foreign L<DBIx::Class> objects, that is ones that originated from a schema that
isn't the underlying schema are currently not supported, but this limitation
may be lifted in the future.

All DBIC operations which may implicitly cause a lookup of a L<KIokuDB> managed
object require live object scope management, just as normal.

It is reccomended to use L<KiokuDB/txn_do> because that will invoke the
appropriate transaction hooks on both layers, as opposed to just in
L<DBIx::Class>.

=head1 SEE ALSO

L<DBIx::Class::KiokuDB>, L<KiokuDB::Backend::DBI>.

=begin Pod::Coverage

define_kiokudb_entries_resultsource
define_kiokudb_gin_index_resultsource
define_kiokudb_schema
kiokudb_handle

=end Pod::Coverage

=cut
