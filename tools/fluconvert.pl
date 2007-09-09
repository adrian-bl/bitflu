#!/usr/bin/perl



use constant AUTOLOAD => "autoload";
use constant TORRENTS => "torrents";
use constant CONFIG   => ".bitflu.config";

use Getopt::Long;
use File::Copy;

use strict;

my %opts = ();
GetOptions(\%opts, "--movedata");

$opts{config} ||= CONFIG;

my $old_dir = $ARGV[0];
my $new_dir = $ARGV[1] or die "Usage: $0 [--config=old_bitflu_conf --movedata] bitflu-0.3xdir newdir\n";
my $config = {};


if(!-d $old_dir) {
	die "Directory '$old_dir' does not exist\n";
}
if(!-d $old_dir."/".TORRENTS) {
	die "$old_dir doesn't look like a bitflu 0.3x storage directory\n";
}
if(((stat($old_dir))[1]) == ((stat($new_dir))[1])) {
	die "$old_dir and $new_dir are the same\n";
}

if(-f $opts{config}) {
	print "# Migrating $opts{config}\n";
	LoadConfig($opts{config},$config);
}
# Set bare-bones config
$config->{workdir} = $new_dir;
$config->{port}        ||= 6688;
$config->{maxup}       ||= 30;
$config->{telnet}      ||= 4001;
$config->{telnetbind}  ||= "127.0.0.1";
$config->{bind}        ||= "0";
$config->{downloaddir} ||= "./downloading";

# Create layout of new home
foreach ($new_dir, $new_dir."/".AUTOLOAD) {
	unless(-d $_) {
		print "# Creating $_\n";
		mkdir($_) or die "Unable to create $_ : $!\n";
	}
}

if($opts{movedata}) {
	print "# This script will MOVE downloaded data for all torrents from $old_dir into $new_dir\n";
	print "# Hit ENTER to continue, CTRL+C to abort\n";
	<STDIN>;
	
	if(!$config->{migrated_from}) {
		die "Pre-Migration did not finish. Please run '$0 @ARGV'\n";
	}
	
	opendir(DLDIR, $old_dir."/$config->{migrated_from}") or die "Unable to open  downloaddir : $!\n";
	foreach my $dirent (readdir(DLDIR)) {
		MigrateData(Hash=>$dirent, Basedir=>$old_dir."/".$config->{migrated_from}, Outdir=>$new_dir."/downloading");
	}
	closedir(DLDIR);
	print "-> Data migrated, you may now start the new version of bitflu.. oh.. don't forget to delete $old_dir !\n";
}
else {
	print "# This script will migrate all torrents from $old_dir into $new_dir\n";
	print "# Hit ENTER to continue, CTRL+C to abort\n";
	<STDIN>;
	if($config->{migrated_from}) {
		die "Do not run me twice with the same options. Use '$0 --movedata @ARGV'\n";
	}
	
	CopyTorrents("$old_dir/torrents", "$new_dir/".AUTOLOAD);
	WriteConfig($config);
	
	print "Next steps:\n";
	print "Step 1: Start the new version of bitflu in the same directory as you started $0\n";
	print "        while starting up, it should pickup all migrated torrents\n";
	print "        hit CTRL+C if you see the 'startup completed' message\n";
	print "Step 2: To migrate the downloaded data, re-run this script as:\n";
	print "        $0 --movedata @ARGV\n";
	print "Good luck!\n";
}


sub MigrateData {
	my(%args) = @_;
	
	my $hash = $args{Hash};
	my $base = $args{Basedir}."/$hash/.done";
	my $obas = "$args{Outdir}/$hash";
	opendir(XDIR,$base) or return;
	print "# Migrating $hash from $base\n";
	
	unless(-f $obas."/.settings/filelayout") {
		print "# Cannot convert '$hash' : New bitflu did not create a filelayout..\n";
		print "# Did you run the new bitflu version before starting this script? You should..\n";
		return;
	}
	
	foreach my $dirent (readdir(XDIR)) {
		my $fp = "$base/$dirent";
		my $fd = "$obas/.done/$dirent";
		next unless -f $fp;
		print "# $hash : Migrating piece $dirent\n";
		foreach my $k qw(done free working exclude) {
			my $torm = $args{Outdir}."/.$k/$dirent";
			unlink($torm);
		}
		move($fp,$fd) or warn "Unable to move $fp to $fd : $!\n";
	}
	close(XDIR);
}


##########################################################
# Write new bare-bones configuration file
sub WriteConfig {
	my($c) = @_;
	my $cfile = << "EOF";
# Migrated configuration file
torrent_port  = $c->{port}
torrent_bind  = $c->{bind}
telnet_port   = $c->{telnet}
telnet_bind   = $c->{telnetbind}
upspeed       = $c->{maxup}
migrated_from = $c->{downloaddir}
EOF

	foreach my $k qw(chroot runas_uid runas_gid workdir) {
		$cfile .= "$k = $c->{$k}\n" if exists($c->{$k});
	}
	
	if( open(CF, ">".CONFIG) ) {
		print CF $cfile;
		close(CF);
		print "# Wrote updated config to ".CONFIG."\n";
	}
	else {
		print "# Unable to write to ".CONFIG." : $!\n";
		print "# Your new configuration file should look like this:\n";
		print $cfile;
	}

}

##########################################################
# Copy .torrent files
sub CopyTorrents {
	my($old,$new) = @_;
	foreach my $dirent (glob("$old/????????????????????????????????????????")) {
		print "# Migrating $dirent\n";
		my ($dst) = (split(/\//,$dirent))[-1];
		copy("$dirent", "$new/$dst") or warn "Unable to copy $old/$dirent to $new/$dst file : $!\n";
	}
}


##########################################################
# Parse existing configuration file
sub LoadConfig {
	my($file,$ref) = @_;
	open(CFH,$file) or die "Unable to read $file : $!\n";
		while(<CFH>) {
			my $line = $_; chomp($line);
			if($line =~ /^#/ or $line =~ /^\s*$/) {
				next; # Comment or empty line
			}
			elsif($line =~ /^([a-zA-Z_]+)\s*=\s*(.*)$/) {
				$ref->{$1} = $2;
			}
		}
	close(CFH);
}
