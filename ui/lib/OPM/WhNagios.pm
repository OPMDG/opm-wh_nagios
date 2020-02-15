package OPM::WhNagios;

# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

use Mojo::Base 'OPM::Plugin';

sub register {
    my ( $self, $app ) = @_;
    return $self;
}

sub register_routes {
    my ( $self, $r, $r_auth, $r_adm ) = @_;

    ## Wh_nagios management
    #
    # show all services, main admin entry
    $r_adm->get('/nagios')->to('wh_nagios-nagios#services')->name('wh_nagios_services');
    # action (cleanup, purge), update retention or delete for multiple services
    $r_adm->post('/nagios')->to('wh_nagios-nagios#services_post')->name('wh_nagios_services_post');
    # show a single service
    $r_adm->route('/nagios/:id', id => qr/\d+/ )->via('GET')->to('wh_nagios-nagios#service')->name('wh_nagios_service');
    # update a single service retention or delete multiple labels
    $r_adm->route('/nagios/:id', id => qr/\d+/ )->via('POST')->to('wh_nagios-nagios#service_post')->name('wh_nagios_service_post');
    # cleanup a single service
    $r_adm->route('/nagios/:id/cleanup', id => qr/\d+/ )->to('wh_nagios-nagios#cleanup')->name('wh_nagios_cleanup');
    # purge a single service
    $r_adm->route('/nagios/:id/purge', id => qr/\d+/ )->to('wh_nagios-nagios#purge')->name('wh_nagios_purge');
    # delete a single service
    $r_adm->route('/nagios/:id/delete', id => qr/\d+/ )->to('wh_nagios-nagios#delete_service')->name('wh_nagios_delete_service');
    # delete a single label
    $r_adm->route('/nagios/:id_s/delete/:id_l', id_s => qr/\d+/, id_l => qr/\d+/ )->to('wh_nagios-nagios#delete_label')->name('wh_nagios_delete_label');

}

sub links_adm_menu {
    my ( $self, $ctrl ) = ( shift, shift );
    my $args  = @_;
    my $value = {
        a => {
            href  => $ctrl->url_for( 'wh_nagios_services' )
        },
        i => {
            class => 'glyphicon glyphicon-cog',
        },
        display => 'wh_nagios'
    };
    return [$value];
}

# Display admin links in graph pages
sub links_service_graph {
    my ( $self, $ctrl ) = ( shift, shift );
    my $arg  = shift;
    my $ret = [];
    my $dbh = $ctrl->database();
    my $sql = $dbh->prepare("SELECT g.id_service, s.service
        FROM public.get_graph( ? ) g
        JOIN public.get_service( g.id_service ) s ON true
        WHERE g.warehouse = 'wh_nagios'"
    );
    $sql->execute( $arg->{id_graph} );
    while ( my $row = $sql->fetchrow_hashref() ) {
        my $value = {
            a => {
                class => 'btn btn-default btn-sm',
                title => $ctrl->l("Admin") . " (" .$row->{service} . ")",
                href  => $ctrl->url_for('wh_nagios_service', id => $row->{id_service})
            },
            i => {
                class => 'fa fa-cog'
            },
            display => ''
        };
        push @{ $ret }, $value;
    }
    return $ret;
}

sub format_state {
    my $whstate = shift ;
    my $class = 'default';

    $class = "success" if $whstate eq "OK";
    $class = "warning" if $whstate eq "WARNING";
    $class = "danger" if $whstate eq "CRITICAL";

    return $class;
}

sub details_service {
    my ( $self, $ctrl ) = ( shift, shift );
    my ($args)  = @_;
    use OPM::WhNagios::Nagios;
    my $whstate = OPM::WhNagios::Nagios::details_service($ctrl, $args->{'id_server'}, $args->{'id_service'});
    my $class = format_state ( $whstate );
    my $value = {
        class  => "label label-$class",
        title => $whstate
    };
    return [$value];
}

sub details_server {
    my ( $self, $ctrl ) = ( shift, shift );
    my ($args)  = @_;
    use OPM::WhNagios::Nagios;
    my $details = OPM::WhNagios::Nagios::details_server($ctrl, $args->{'id_server'});
    my $values = [];
    push (@{$values}, {
        class => '',
        title => ''
    });
    if ( @{$details} eq 1 and @{$details}[0]->{'state'} eq 'OK' ){
        my $class = format_state( @{$details}[0]->{'state'} );
        push (@{$values}, {
        class  => "label label-$class",
        title => '<i class="glyphicon glyphicon-thumbs-up"></i> ' . $ctrl->l('Good job !')
        });
    } else {
        foreach my $detail ( @{$details} ) {
            my $class = format_state ( $detail->{'state'} );
            push (@{$values}, {
                class  => "label label-$class",
                title => "$detail->{'state'} : $detail->{'num'}"
            });
        }
        }
    return $values;
}

1;
