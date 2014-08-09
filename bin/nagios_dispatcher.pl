#!/usr/bin/perl -w
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2014: Open PostgreSQL Monitoring Development Group


=head1 About

Authors :
  Open PostgreSQL Monitoring Development Group

Version 1.0 : 2012-11-19
Version 1.1 : 2012-12-28

Name : nagios_dispatcher.pl

=head1 SYNOPSIS

  nagios_dispatcher.pl [--daemon] [--verbose] [--directory=data_dir] [--frequency=scrutinizing_frequency] --config=configuration_file

=head1 USE

This program scrutinizes nagios' perfdata directory, or any other similarly set up directory

Spooled files have to be of this format (in the nagios configuration) :

  host_perfdata_file_template=DATATYPE::HOSTPERFDATA\tTIMET::$TIMET$\tHOSTNAME::$HOSTNAME$\tHOSTPERFDATA::$HOSTPERFDATA$\tHOSTCHECKCOMMAND::$HOSTCHECKCOMMAND$\tHOSTSTATE::$HOSTSTATE$\tHOSTSTATETYPE::$HOSTSTATETYPE$\tHOSTOUTPUT::$HOSTOUTPUT$

  service_perfdata_file_template=DATATYPE::SERVICEPERFDATA\tTIMET::$TIMET$\tHOSTNAME::$HOSTNAME$\tSERVICEDESC::$SERVICEDESC$\tSERVICEPERFDATA::$SERVICEPERFDATA$\tSERVICECHECKCOMMAND::$SERVICECHECKCOMMAND$\tHOSTSTATE::$HOSTSTATE$\tHOSTSTATETYPE::$HOSTSTATETYPE$\tSERVICESTATE::$SERVICESTATE$\tSERVICESTATETYPE::$SERVICESTATETYPE$\tSERVICEOUTPUT::$SERVICEOUTPUT$

=cut

use strict;

use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use POSIX qw(setsid);
use DBI;

my $verbose = 0;

# These are globals. No point in sending them from function to function
my $connection_string;
my $user;
my $password;
my $debug  = 0;
my $syslog = 0;

# The next two functions were stolen from Nagios::Plugin::Performance
# They do the parsing of the PERFDATA string

my $value                        = qr/[-+]?[\d\.,]+/;
my $value_re                     = qr/$value(?:e$value)?|NaN/;
my $value_with_negative_infinity = qr/$value_re|~/;

sub parse_perfrecord {
    my $string = shift;
    $string
        =~ /^'?([^'=]+)'?=($value_re)([\w%]*);?($value_with_negative_infinity\:?$value_re?)?;?($value_with_negative_infinity\:?$value_re?)?;?($value_re)?;?($value_re)?/o;
    return undef
        unless ( ( defined $1 && $1 ne "" ) && ( defined $2 && $2 ne "" ) );
    my @info = ( $1, $2, $3, $4, $5, $6, $7 );

    # We convert any commas to periods, in the value fields
    map { defined $info[$_] && $info[$_] =~ s/,/./go } ( 1, 3, 4, 5, 6 );

    # Check that $info[1] is an actual value
    # We do this by returning undef if a warning appears
    my $performance_value;
    {
        my $not_value;
        local $SIG{__WARN__} = sub { $not_value++ };
        $performance_value = $info[1] + 0;
        return undef if $not_value;
    }
    my $p = {
        label    => $info[0],
        value    => $performance_value,
        uom      => $info[2],
        warning  => $info[3],
        critical => $info[4],
        min      => $info[5],
        max      => $info[6]
    };
    return $p;
}

sub parse_perfstring {
    my ($perfstring) = @_;
    my @perfs = ();
    my $p;
    while ($perfstring) {
        $perfstring =~ s/^\s*//;

    # If there is more than 1 equals sign, split it out and parse individually
        if ( @{ [ $perfstring =~ /=/g ] } > 1 ) {
            $perfstring =~ s/^(.*?=.*?)\s//;
            if ( defined $1 ) {
                $p = parse_perfrecord($1);
            }
            else {

                # This could occur if perfdata was soemthing=value=
                # Since this is invalid, we reset the string and continue
                $perfstring = "";
                $p          = parse_perfrecord($perfstring);
            }
        }
        else {
            $p          = parse_perfrecord($perfstring);
            $perfstring = "";
        }
        push @perfs, $p if $p;
    }
    return \@perfs;
}

sub log_message {
    my ($message) = @_;
    if ($syslog) {
        setlogsock('unix');
        openlog( 'nagios_dispatcher', '', 'user' );
        syslog( 'info', $message );
        closelog();
    }
    else {
        print STDERR $message, "\n";
    }
}

# This function splits a line of performance data and parses it.
# Parsing of the perfdata part is done by parse_perfstring

sub parse_perfline {
    my ($line) = @_;
    my %parsed;

    # Performance lines are made of KEY::VALUE\tKEY::VALUE...
    my @elements = split( "\t", $line );
    unless ( @elements > 0 ) {

        $debug and die "FATAL: Can't understand this line : <$line>\n";
        log_message "ERROR: Can't understand this line : <$line>";

        return;
    }
    foreach my $element (@elements) {

        # This has to be a key-value. Else I die !
        unless ( $element =~ /^(\S+)::(.*)$/ ) {

            $debug
                and die
                "FATAL: Can't understand this attribute : <$element>\n";
            log_message
                "ERROR: Can't understand this attribute : <$element>";

            return;
        }
        $parsed{$1} = $2;
    }

    # Ok. Is it a serviceperfdata or a hostperfdata ?
    # we consider a hostperfdata a serviceperfdata of a certain kind: as a
    # service desc, it will be host
    if ( $parsed{DATATYPE} eq 'HOSTPERFDATA' ) {
        $parsed{SERVICEDESC}     = 'HOST';
        $parsed{SERVICEPERFDATA} = $parsed{HOSTPERFDATA};
        $parsed{SERVICEOUTPUT}   = $parsed{HOSTOUTPUT};
        $parsed{SERVICESTATE}    = $parsed{HOSTSTATE};
        undef $parsed{HOSTPERFDATA};
        undef $parsed{HOSTOUTPUT};
        undef $parsed{HOSTSTATE};
    }

    # Now everything is the same kind of performance data
    # Let's split the perfdata
    my $perfsref = parse_perfstring( $parsed{SERVICEPERFDATA} );

    # We store in the same hash the parsed version of the performance data
    # With a reference to @perfs. This way we have a complete parsed structure
    # of the file
    $parsed{SERVICEPERFDATA_PARSED} = $perfsref;
    return \%parsed;
}

# Simple function to convert MB to B, Kib to b, returning the base unit and
# multiplying factor
sub eval_uom {
    my ($uom) = @_;
    return ( '', 1 ) unless $uom;    # No uom
    my $multfactor;
    my $basic_uom;

    # Okay, is it starting with ki, Mi, Gi, Ti, k, M, G, T ?
    # Repetitive but simple code
    if ( $uom =~ /^ki(.*)/ ) {
        $multfactor = 1000;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^k(.*)/ ) {
        $multfactor = 1024;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^Mi(.*)/ ) {
        $multfactor = 1000 * 1000;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^M(.*)/ ) {
        $multfactor = 1024 * 1024;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^Gi(.*)/ ) {
        $multfactor = 1000 * 1000 * 1000;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^G(.*)/ ) {
        $multfactor = 1024 * 1024 * 1024;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^Ti(.*)/ ) {
        $multfactor = 1000 * 1000 * 1000 * 1000;
        $basic_uom  = $1;
    }
    elsif ( $uom =~ /^T(.*)/ ) {
        $multfactor = 1024 * 1024 * 1024 * 1024;
        $basic_uom  = $1;
    }
    else {

        # I don't understand this unit. Let's keep it as is
        $multfactor = 1;
        $basic_uom  = $uom;
    }
    return ( $basic_uom, $multfactor );
}

# This function reads a file line by line and calls parse_perfline for each one
# It then returns an array with an element per counter
sub read_file {
    my ($filename) = @_;
    my $fh;
    my @parsed_file;
    open( $fh, $filename ) or die "FATAL: Can't open '$filename' : $!\n";

    while ( my $line = <$fh> ) {
        my $parsed_line = parse_perfline($line);

 # We want to return an array of perfcounters (hash). We are interested in
 # TIMET, HOSTNAME, SERVICEDESC, SERVICESTATE
 # every label and value element of SERVICEPERFDATA_PARSED
 # So we push that into @parsed_file
 # Lots of values can be undef below. We remove the use strict for a few lines
        no warnings;
        foreach
            my $perfcounterref ( @{ $parsed_line->{SERVICEPERFDATA_PARSED} } )
        {
            my %perfcounter;
            $perfcounter{TIMET}        = $parsed_line->{TIMET};
            $perfcounter{HOSTNAME}     = $parsed_line->{HOSTNAME};
            $perfcounter{SERVICEDESC}  = $parsed_line->{SERVICEDESC};
            $perfcounter{SERVICESTATE} = $parsed_line->{SERVICESTATE};
            $perfcounter{LABEL}        = $perfcounterref->{label};

            # Okay, lets work on the units. We normalize everything
            my ( $basic_uom, $multfactor )
                = eval_uom( $perfcounterref->{uom} );
            $perfcounter{VALUE}   = $perfcounterref->{value} * $multfactor;
            $perfcounter{MIN}     = $perfcounterref->{min} * $multfactor;
            $perfcounter{MAX}     = $perfcounterref->{max} * $multfactor;
            $perfcounter{WARNING} = $perfcounterref->{warning} * $multfactor;
            $perfcounter{CRITICAL}
                = $perfcounterref->{critical} * $multfactor;
            $perfcounter{UOM} = $basic_uom;

            if ($verbose) {
                $perfcounter{ORIG_UOM}   = $perfcounterref->{uom};
                $perfcounter{ORIG_VALUE} = $perfcounterref->{value};
                $perfcounter{MULTFACTOR} = $multfactor;
            }

            # Done. We push it into our array of results
            push @parsed_file, \%perfcounter;
        }
        use warnings;
    }
    $verbose and log_message( Dumper( \@parsed_file ) );
    return \@parsed_file;
}

# Daemonize function : fork, kill the father, detach from console, go to root
sub daemonize {
    my $child = fork();
    if ($child) {

        # I'm the father
        #
        exit 0;
    }
    close(STDIN);
    close(STDOUT);
    close(STDERR);
    open STDOUT, ">/dev/null";
    open STDERR, ">/dev/null";
    POSIX::setsid();
    chdir '/';

    log_message "Daemonized."
}

# Remove all records that match any filter
sub do_filter {
    my ( $data, $hostname_filter, $service_filter, $label_filter ) = @_;
    my $new_data;
    foreach my $counter ( @{$data} ) {
        next if ( $counter->{HOSTNAME}    =~ $hostname_filter );
        next if ( $counter->{SERVICEDESC} =~ $service_filter );
        next if ( $counter->{LABEL}       =~ $label_filter );
        push @$new_data, ($counter);
    }
    return $new_data;
}

## Database access functions

# This function connects to the database and returns a db handle
sub dbconnect {
    my $dbh;
    my $retry = 0;

    while ( not defined $dbh ) {
        # We sleep longer and longer in case of failures
        sleep $retry;

        $retry++;

        $dbh = DBI->connect( $connection_string, $user, $password )
            or log_message
                sprintf("ERROR: Couldn't connect to '%s' with user '%s'!\n"
                    . "Retry in $retry seconds...", $connection_string, $user);

    }

    $debug and log_message
        sprintf("DEBUG: Connected to '%s' with user '%s'.\n",
            $connection_string, $user);

    return $dbh;
}

# This function inserts parsed data into the database
sub insert_parsed_data {
    my ( $dbh, $parsed_data, $filename ) = @_;
    my $hub_seq = 'wh_nagios.hub_id_seq';

    unless ( defined $dbh ) {
        log_message("ERROR: No connection to insert parsed data!");
        return 0;
    }

    # get a batch number
    my $ref_idbatch = $dbh->selectall_arrayref(
        sprintf( "SELECT nextval('%s')", $hub_seq ) );

    unless ($ref_idbatch) {
        log_message("ERROR: Couldn't get a batch id from sequence $hub_seq!");
        return 0;    #Failure
    }

    my $batch_number = $ref_idbatch->[0]->[0];

    $debug
        and log_message
        sprintf( 'DEBUG: Processing batch #%u.', $batch_number );

    my $sth = $dbh->do('COPY wh_nagios.hub (id, data) FROM STDIN');

    foreach my $record ( @{$parsed_data} ) {
        my $copy    = "$batch_number\t";
        my @counter = (%$record);

        #Add double quotes around each attribute and value:
        @counter = map { '"' . $_ . '"' } @counter;

        # Make an array text representation out of it
        $copy .= '{' . join( ',', @counter ) . '}' . "\n";

        # Send it to copy
        my $executed = $dbh->pg_putcopydata($copy);
        unless ($executed) {
            log_message sprintf(
                "ERROR: Can't COPY this record: "
                    . "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s.\n"
                    . "File: '%s'",
                $record->{HOSTNAME},    $record->{TIMET},
                $record->{SERVICEDESC}, $record->{SERVICESTATE},
                $record->{LABEL},       $record->{VALUE},
                $record->{MIN},         $record->{MAX},
                $record->{WARNING},     $record->{CRITICAL},
                $record->{UOM},         $filename
            );
            return 0;    #Failure
        }
    }

    $dbh->pg_putcopyend();

    $debug and log_message sprintf( 'DEBUG: Batch #%u done.', $batch_number );

    return 1;            # Ok
}

## Watch the incoming directory

# As soon as a file is there, send it to read_file.
sub watch_directory {
    my ( $dirname, $frequency, $hostname_filter, $service_filter,
        $label_filter )
        = @_;
    my $num_files = 0;

    log_message "LOG: start working.";

    while (1) {
        my $dir;
        my $dbh = undef;

        opendir( $dir, $dirname )
            or die "FATAL: Can't open directory $dirname: $!\n";

    LOOP_DIR: while ( my $entry = readdir $dir ) {
            next if ( $entry =~ '^\.' );

            $dbh or $dbh = dbconnect(); # We reconnect for each batch of files

            my $parsed = read_file("$dirname/$entry");

            # Get rid of records that should be filtered
            $parsed = do_filter( $parsed, $hostname_filter, $service_filter,
                $label_filter );

            $dbh->begin_work();
            my $inserted
                = insert_parsed_data( $dbh, $parsed, "$dirname/$entry" );

            # If not inserted, we retry
            unless ($inserted) {
                $dbh->disconnect();
        undef $dbh;

                log_message
                    "ERROR: Could not insert '$entry' datas. Retrying.";

                redo LOOP_DIR;
            }

            $debug and log_message "Entry '$entry' processed.";

            unlink("$dirname/$entry")
                or die "FATAL: Can't remove '$dirname/$entry': $!\n";

            $num_files++;

            $debug and log_message "DEBUG: File '$dirname/$entry' processed.";

            $dbh->commit();
        }

        # database handle's garbage collector
        $dbh and $dbh->disconnect();
        undef $dbh;

        # log our activity every 5 minutes
        my ($sec, $min) = (localtime())[0,1];
        log_message sprintf("LOG: Processed %u nagios performance files so far.", $num_files)
            if ($sec + ($min%5)*60) < $frequency%60;

        sleep $frequency;
    }
}

# This function parses the configuration file and modifies variables
# It is hand made and very dumb, due to the simple configuration file
sub parse_config {
    my ($config,         $refdaemon,             $refdirectory,
        $reffrequency,   $ref_connection_string, $ref_user,
        $ref_password,   $ref_syslog,            $ref_debug,
        $ref_hostfilter, $ref_servfilter,        $ref_lablfilter,
        $uid, $gid
    ) = @_;

    my $confH;
    open $confH, $config or die "Can't open <$config>:$!\n";

    while ( my $line = <$confH> ) {
        chomp $line;

        #It's a simple ini file
        $line =~ s/\s*#.*//;    # Remove comments, and spaces before them

        next if ( $line eq '' );    # Ignore empty lines

        $line =~ s/=\s+/=/;         # Remove spaces after the first =
        $line =~ s/\s+=/=/;         # Remove spaces before the first =

        $line =~ /^(.*?)=(.*)$/ or die "Can't parse <$line>\n";

        my $param = $1;
        my $value = $2;

        if ( $param eq 'daemon' ) {
            $$refdaemon = $value;
        }
        elsif ( $param eq 'directory' ) {
            $$refdirectory = $value;
        }
        elsif ( $param eq 'frequency' ) {
            $$reffrequency = $value;
        }
        elsif ( $param eq 'db_connection_string' ) {
            $$ref_connection_string = $value;
        }
        elsif ( $param eq 'db_user' ) {
            $$ref_user = $value;
        }
        elsif ( $param eq 'db_password' ) {
            $$ref_password = $value;
        }
        elsif ( $param eq 'syslog' ) {
            $$ref_syslog = $value;
            if ($value) {

                # We will need this module
                use Sys::Syslog qw( :DEFAULT setlogsock);
            }
        }
        elsif ( $param eq 'debug' ) {
            $$ref_debug = $value;
        }
        elsif ( $param eq 'hostname_filter' ) {
            $value =~ m|^/(.*)/$|;
            $$ref_hostfilter = qr/$1/;
        }
        elsif ( $param eq 'service_filter' ) {
            $value =~ m|^/(.*)/$|;
            $$ref_servfilter = qr/$1/;
        }
        elsif ( $param eq 'label_filter' ) {
            $value =~ m|^/(.*)/$|;
            $$ref_lablfilter = qr/$1/;
        }
        elsif ( $param eq 'uid' ) {
            $$uid = $value;
        }
        elsif ( $param eq 'gid' ) {
            $$gid = $value;
        }
        else {
            die "Unknown parameter '$param' in configuration file\n";
        }
    }
    close $confH;
}

# Main

# Catch die() call to write it to syslog.
$SIG{__DIE__} = sub {
    &log_message;
    die;
};

# Command line options
my $daemon;
my $directory;
my $frequency;
my $help;
my $config;
my $hostname_filter;
my $service_filter;
my $label_filter;
my $uid;
my $gid;

my $result = GetOptions(
    "daemon"      => \$daemon,
    "verbose"     => \$verbose,
    "directory=s" => \$directory,
    "frequency=i" => \$frequency,
    "config=s"    => \$config,
    "help"        => \$help
);

# Usage if help asked for
Pod::Usage::pod2usage( -exitval => 1, -verbose => 3 ) if ($help);

# Usage if no configuration file or wrong parameters
Pod::Usage::pod2usage( -exitval => 1, -verbose => 1 )
    unless ( $config and $result );

# Parse config file
parse_config(
    $config,           \$daemon,            \$directory,
    \$frequency,       \$connection_string, \$user,
    \$password,        \$syslog,            \$debug,
    \$hostname_filter, \$service_filter,    \$label_filter,
    \$uid, \$gid
);

# Usage if missing parameters in command line or configuration file
Pod::Usage::pod2usage( -exitval => 1, -verbose => 1 ) unless ($directory);

# Add default values if they are still not set up
$frequency = 5 unless $frequency;

daemonize if $daemon;

## drop root if asked
# start with group rights
if ( defined $gid ) {
    my $oldgid = $(;

    die("Invalid GID: $gid.") if $gid < 0;

    $( = $gid;        # GID
    $) = "$gid $gid"; # EGID

    die("Could not set GIDs ($() to '$gid'.")
        if $( ne "$gid $gid";

    log_message("Groups privileges dropped from '$oldgid' to '$('");
}

# drop user rights now
if ( defined $uid ) {
    my $olduid = $<;

    die("Invalid UID: $uid.") if $uid < 0;

    $< = $> = $uid; # UID, EUID

    die("Could not set UID ($<) to '$uid'.")
        if $< != $uid;

    log_message("User privileges dropped from '$olduid' to '$<'");
}

# Let's work
watch_directory(
    $directory,      $frequency, $hostname_filter,
    $service_filter, $label_filter
);

=head1 COPYRIGHT

parse_perfrecord and parse_perfstring are

Copyright (C) 2006-2007 Nagios Plugin Development Team

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

the rest is

This program is open source, licensed under the PostgreSQL License.
Copyright (C) 2012-2014 Open PostgreSQL Monitoring Development Group



=cut
