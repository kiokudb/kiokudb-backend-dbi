package KiokuDB::TypeMap::Entry::DBIC::Schema;
use Moose;
# ABSTRACT: KiokuDB::TypeMap::Entry for DBIx::Class::Schema objects.

use Scalar::Util qw(weaken refaddr);

use namespace::autoclean;

with qw(KiokuDB::TypeMap::Entry);

sub compile {
    my ( $self, $class ) = @_;

    return KiokuDB::TypeMap::Entry::Compiled->new(
        collapse_method => sub {
            my ( $collapser, @args ) = @_;

            $collapser->collapse_first_class(
                sub {
                    my ( $collapser, %args ) = @_;

                    if ( refaddr($collapser->backend->schema) == refaddr($args{object}) ) {
                        return $collapser->make_entry(
                            %args,
                            data => undef,
                            meta => {
                                immortal => 1,
                            },
                        );
                    } else {
                        croak("Referring to foreign DBIC schemas is unsupported");
                    }
                },
                @args,
            );
        },
        expand_method => sub {
            my ( $linker, $entry ) = @_;

            my $schema = $linker->backend->schema;

            $linker->register_object( $entry => $schema, immortal => 1 );

            return $schema;
        },
        id_method => sub {
            my ( $self, $object ) = @_;

            return 'dbic:schema'; # singleton
        },
        refresh_method => sub { },
        entry => $self,
        class => $class,
    );
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__

=pod

=head1 DESCRIPTION

This typemap entry handles references to L<DBIx::Class::Schema> as a scoped
singleton.

The ID of the schema is always C<dbic:schema>.

References to L<DBIx::Class::Schema> objects which are not a part of the
underlying L<DBIx::Class> layout are currently not supported, but may be in the
future.

=begin Pod::Coverage

compile

=end Pod::Coverage

=cut
