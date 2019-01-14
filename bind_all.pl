#!/usr/bin/perl -w
# --
# $Header$
#
#                             (|)
#                           _/{|}\_
#                         \\  - -  //
#                          (  @ @  )
#O-----------------------oOOo-(_)-oOOo--------------------O
#|          file            |\_x_/|                       |
#|by                        (|||||)                       |
#|   sham1316@gmail.com                                   |
#|                                Oooo                    |
#O------------------------oooO---(   )--------------------O
#                        (   )    ) /
#                         \ (    (_/
#                          \_)
# --
#    sham1316@gmail.com
# --

use Devel::Size qw(size total_size); 

use warnings;
use strict;
use POSIX;
use IO::Seekable;
use IO::Socket;
use IO::Select;
use IO::Handle;
use DBD::mysql;
use Time::Local;
use Fcntl ':flock'; 

sub lock{ 
    my $file = shift;
    open(my $f, ">", $file) || die "can`t lock file $file: $!";
    flock($f,LOCK_EX || LOCK_NB) or die "Lock failed $!";;
    return $f; 
} 
    
sub unlock{ 
    my $file = shift;

    flock($file,LOCK_UN); 
    unlink($file); 
    close($file); 
}

=item pursedate($1)
    parse data - format 29-Jul-2011 13:46:56.541 - to_unixTime
=cut
sub parsedate{
    my $d = shift;

    my %mon2num = qw(
	jan 1  feb 2  mar 3  apr 4  may 5  jun 6
	jul 7  aug 8  sep 9  oct 10 nov 11 dec 12
	);
    
    my ($mday,$mon,$year,$hour,$min,$sec)=($d=~/(\d+)-(\w+)-(\d+) (\d+):(\d+):(\d+)\./);
    return timelocal($sec,$min,$hour,$mday,$mon2num{lc $mon}-1,$year);;
}

my $pid_file="/var/named/var/stats/query_stats.pid";
my $query_file="/var/named/var/log/query.log";
#$query_file="/var/named/var/log/ttt";
my $time_to_sleep=2;
my $my_user="statdns";
my $my_pass="8VUn52je6ZYbywMW";
my $my_base="statdns";
my $my_host="80.253.27.101";
my $time_to_die = 0;


sub signal_handler{
    unlock($pid_file);
    print "time_to_die!!!!\n";
    $time_to_die = 1;
}
sub check_exist{
    my ($dbh, $date, $clientIP, $query, $serverIP) = @_;
    
    my $sqlQuery = sprintf("SELECT count(*)  
                            FROM `queryes`
                            where `Date` = ? and
	                	`clientIP` = INET_ATON(?) and
				`query` = ? and 
				`serverIP` = INET_ATON(?)
				limit 1");
    my %attr = (
            PrintError => 0,
	    RaiseError => 1,
	         );
    my @result = $dbh->selectrow_array($sqlQuery, \%attr, ($date, $clientIP, $query, $serverIP));
    return 0;
}

$SIG{INT} = $SIG{TERM} = $SIG{HUP} = \&signal_handler;
$SIG{PIPE}='IGNORE';

my $pid = fork;
exit if $pid;
my $phf = lock($pid_file);
print $phf "$$";
$phf->flush;

die "Couldn`t fork: $!" unless defined($pid);

for my $handle(*STDIN, *STDOUT, *STDERR){
    open($handle, "+<", "/dev/null") || die "can't reonen $handle to /dev/null: $!";
}

for my $handle(*STDOUT, *STDERR){
    open($handle, ">", "/var/named/var/stats/queryes-logger.log") || die "can't reonen $handle to /dev/null: $!";
}

POSIX::setsid() or die "Can`t start new session: $!";

my $fh;
my $inode_old = 0;
my $dbh;

until($time_to_die){
    eval{
	$dbh->disconnect;
    };
    my $inode_new = (stat($query_file))[1];
    if ($inode_old != $inode_new){
	print "reopen file!\n";
        open($fh, '<', $query_file) or die $!;
	seek($fh,0,SEEK_END) if(0 == $inode_old); }
    else{ 
	sleep $time_to_sleep;
    }
    seek($fh,0,1);
    my @records = ();
    $inode_old = $inode_new;
    #connect to mysql database
    $dbh = DBI->connect("DBI:mysql:database=" . $my_base . ";host=" . $my_host,
                          $my_user, $my_pass,{'RaiseError' => 1}) or die "Unable to connect: $DBI::errstr\n";								    
    while(<$fh>){
	my @record = ();
        if (/^(.+) client (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})#\d{1,5}: view (.+): query: (.+) (.+) (.+) (.+) \((\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\)$/){
		#$1 - date, $2 - clientIP, $3 - view, $4 - query, $5 - type1, $6 type2, $7 - type3, $8 - serverIP
	    @record = (parsedate($1), $2, $3, $4 ,$5, $6, $7, $8, undef);
        }
	else{
	    @record = (undef, undef, undef, undef, undef, undef, undef, undef, $_);
        }
	push(@records,\@record);
#	if(1 => $#records+1){
#	    next if check_exist($dbh, $record[0], $record[1], $record[3], $record[7]);}
    }
    print "insert records!\n";
    my $sth=$dbh->prepare("insert DELAYED into queryes values(?,INET_ATON(?),?,?,?,?,?,INET_ATON(?),?)");
    foreach (@records){
	$sth->execute(@{$_}) || die "Couldn't insert record : $DBI::errstr";
    }
}
