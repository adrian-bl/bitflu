#!/usr/bin/perl -w
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use Data::Dumper;
use Getopt::Long;



my $bitflu_run           = undef;     # Start as not_running and not_killed
my $getopts              = { help => undef, config => '.bitflu.config', version => undef };
$SIG{PIPE}  = $SIG{CHLD} = 'IGNORE';
$SIG{INT}   = $SIG{HUP}  = $SIG{TERM} = \&HandleShutdown;

GetOptions($getopts, "help|h", "version", "plugins", "config=s") or exit 1;
if($getopts->{help}) { die "Usage: $0 [--config=.bitflu.config --version --help --plugins]\n"; }


# -> Create bitflu object
my $bitflu = Bitflu->new(configuration_file=>$getopts->{config}) or Carp::confess("Unable to create Bitflu Object");
if($getopts->{version}) { die $bitflu->_Command_Version->{MSG}->[0]->[1]."\n" }



my @loaded_plugins = $bitflu->LoadPlugins('Bitflu');
if($getopts->{plugins}) {
	print "# Loaded Plugins: (from ".$bitflu->Configuration->GetValue('plugindir').")\n";
	foreach (@loaded_plugins) { printf("File %-35s provides: %s\n", $_->{file}, $_->{package}); }
	exit(0);
}

$bitflu->SysinitProcess();
$bitflu->InitPlugins();
$bitflu->PreloopInit();

$bitflu_run = 1 if !defined($bitflu_run); # Enable mainloop and sighandler if we are still not_killed

while($bitflu_run == 1) {
	foreach my $x (@{$bitflu->{_Runners}}) {
		$x->run();
	}
	select(undef,undef,undef,$bitflu->Configuration->GetValue('sleeper'));
}

$bitflu->info("-> Shutdown completed after running for ".(int(time())-$bitflu->{_BootTime})." seconds");
exit(0);


sub HandleShutdown {
	my($sig) = @_;
	if(defined($bitflu_run) && $bitflu_run == 1) {
		# $bitflu is running, so we can use ->info
		$bitflu->info("-> Starting shutdown... (signal $sig received)");
	}
	else {
		print "-> Starting shutdown... (signal $sig received), please wait...\n";
	}
	$bitflu_run = 0; # set it to not_running and killed
}







package Bitflu;
use strict;
use Carp;
use constant VERSION => "0.42-Stable (20071224)";

	##########################################################################
	# Create a new Bitflu-'Dispatcher' object
	sub new {
		my($class, %args) = @_;
		my $self = {};
		bless($self, $class);
		$self->{Core}->{Configuration}  = Bitflu::Configuration->new(super=>$self, configuration_file => $args{configuration_file});
		$self->{Core}->{Network}        = Bitflu::Network->new(super => $self);
		$self->{Core}->{AdminDispatch}  = Bitflu::Admin->new(super => $self);
		$self->{Core}->{QueueMgr}       = Bitflu::QueueMgr->new(super => $self);
		$self->{Core}->{Sha1}           = Bitflu::Sha1->new(super => $self);
		$self->{_Runners}               = ();
		$self->{_BootTime}              = time();
		$self->{_Plugins}               = ();
		return $self;
	}
	
	##########################################################################
	# Call hardcoded configuration plugin
	sub Configuration {
		my($self) = @_;
		return $self->{Core}->{Configuration};
	}
	
	##########################################################################
	# Call hardcoded sha1 plugin
	sub Sha1 {
		my($self) = @_;
		return $self->{Core}->{Sha1};
	}
	
	##########################################################################
	# Call hardcoded Network IO plugin
	sub Network {
		my($self) = @_;
		return $self->{Core}->{Network};
	}
	
	##########################################################################
	# Call hardcoded Admin plugin
	sub Admin {
		my($self) = @_;
		return $self->{Core}->{AdminDispatch};
	}
	
	##########################################################################
	# Call hardcoded Queue plugin
	sub Queue {
		my($self) = @_;
		return $self->{Core}->{QueueMgr};
	}
	
	##########################################################################
	# Call currently loaded storage plugin
	sub Storage {
		my($self) = @_;
		return $self->{Plugin}->{Storage};
	}
	
	##########################################################################
	# Let bitflu run the given target
	sub AddRunner {
		my($self,$target) = @_;
		push(@{$self->{_Runners}},$target);
	}
	
	
	##########################################################################
	# Register the exclusive storage plugin
	sub AddStorage {
		my($self,$target) = @_;
		if(defined($self->{Plugin}->{Storage})) { $self->panic("Unable to register additional storage driver '$target' !"); }
		$self->{Plugin}->{Storage} = $target;
		$self->debug("AddStorage($target)");
		return 1;
	}
	
	
	
	##########################################################################
	# Loads all plugins from 'plugins' directory but does NOT init them
	sub LoadPlugins {
		my($self,$xclass) = @_;
		#
		unshift(@INC, $self->Configuration->GetValue('plugindir'));
		my $pdirpath = $self->Configuration->GetValue('plugindir')."/$xclass";
		my @plugins  = ();
		
		opendir(PLUGINS, $pdirpath) or $self->stop("Unable to read directory '$pdirpath' : $!");
		foreach my $dirent (sort readdir(PLUGINS)) {
			next unless $dirent =~ /^((\d\d)_(.+)\.pm)$/i;
			push(@plugins, {file=>$1, order=>$2, class=>$xclass, modname=>$3, package=>$xclass."::".$3});
			$self->debug("Found plugin $plugins[-1]->{package} in folder $pdirpath");
		}
		close(PLUGINS);
		
		$self->{_Plugins} = \@plugins;
		
		foreach my $plugin (@{$self->{_Plugins}}) {
			my $fname = $plugin->{class}."/".$plugin->{file};
			$self->debug("Loading $fname");
			eval { require $fname; };
			if($@) {
				my $perr = $@; chomp($perr);
				$self->warn("Unable to load plugin '$fname', error was: '$perr'");
				$self->stop(" -> Please fix or remove this broken plugin file from $pdirpath");
			}
		}
		return @plugins;
	}
	
	##########################################################################
	# Startup all plugins
	sub InitPlugins {
		my($self) = @_;
		
		my @TO_INIT = ();
		foreach my $plugin (@{$self->{_Plugins}}) {
			$self->debug("Registering '$plugin->{package}'");
			my $this_plugin = $plugin->{package}->register($self) or $self->panic("Regsitering '$plugin' failed, aborting");
			push(@TO_INIT, {name=>$plugin->{package}, ref=>$this_plugin});
		}
		foreach my $toinit (@TO_INIT) {
			$self->debug("Firing up '$toinit->{name}'");
			$toinit->{ref}->init() or $self->panic("Unable to init plugin : $!");
		}
		foreach my $coreplug (sort keys(%{$self->{Core}})) {
			$self->debug("Starting Core-Plugin '$coreplug'");
			$self->{Core}->{$coreplug}->init() or $self->panic("Unable to init Core-Plugin : $!");
		}
	}
	
	##########################################################################
	# Change nice level, chroot and drop privileges
	sub SysinitProcess {
		my($self) = @_;
		
		my $chroot = $self->Configuration->GetValue('chroot');
		my $uid    = int($self->Configuration->GetValue('runas_uid') || 0);
		my $gid    = int($self->Configuration->GetValue('runas_gid') || 0);
		my $renice = int($self->Configuration->GetValue('renice')    || 0);
		
		# Lock values because we cannot change them after we finished
		foreach my $lockme qw(runas_uid runas_gid chroot) {
			$self->Configuration->RuntimeLockValue($lockme);
		}
		
		
		# -> Set niceness (This is done before dropping root to get negative values working)
		if($renice) {
			$renice = ($renice > 19 ? 19 : ($renice < -20 ? -20 : $renice) ); # Stop funny stuff...
			$self->info("Setting my own niceness to $renice");
			POSIX::nice($renice) or $self->warn("nice($renice) failed: $!");
		}
		
		# -> Chroot
		if(defined($chroot)) {
			$self->info("Chrooting into '$chroot'");
			Carp::longmess("FULLY_LOADING_CARP");
			chdir($chroot)  or $self->panic("Cannot change into directory '$chroot' : $!");
			chroot($chroot) or $self->panic("Cannot chroot into directory '$chroot' (are you root?) : $!");
			chdir('/')      or $self->panic("Unable to change into new chroot topdir: $!");
		}
		
		# -> Drop group privileges
		if($gid) {
			$self->info("Changing gid to $gid");
			$! = undef;
			$) = "$gid $gid";
			$self->panic("Unable to set EGID: $!") if $!;
			$( = "$gid";
			$self->panic("Unable to set GID: $!")  if $!;
		}
		
		# -> Drop user privileges
		if($uid) {
			$self->info("Changing uid to $uid");
			POSIX::setuid($uid) or $self->panic("Unable to change UID: $!");
		}
		
		# -> Check if we are still root. We shouldn't.
		if($> == 0 or $) == 0) {
			$self->warn("Refusing to run with root privileges. Do not start $0 as root unless you are using");
			$self->warn("the chroot option. In this case you must also specify the options runas_uid & runas_gid");
			$self->stop("Bitflu refuses to run as root");
		}
		
		$self->info("$0 is running with pid $$ ; uid = ($>|$<) / gid = ($)|$()");
	}
	
	
	##########################################################################
	# This should get called after starting the mainloop
	# The subroutine does the same as a 'init' in a plugin
	sub PreloopInit {
		my($self) = @_;
		$self->Admin->RegisterCommand('die'      , $self, '_Command_Shutdown'     , 'Terminates bitflu');
		$self->Admin->RegisterCommand('version'  , $self, '_Command_Version'      , 'Displays bitflu version string');
		$self->Admin->RegisterCommand('date'     , $self, '_Command_Date'         , 'Displays current time and date');
		$self->Admin->RegisterCommand('sysinfo'  , $self, '_Command_Sysinfo'      , 'Returns various system related informations');
	}
	
	
	##########################################################################
	# bye!
	sub _Command_Shutdown {
		my($self) = @_;
		kill(2,$$);
		return {CHAINSTOP=>1, MSG=>[ [1, "Shutting down $0 (with pid $$)"] ]};
	}

	##########################################################################
	# Return version string
	sub _Command_Version {
		my($self) = @_;
		return {CHAINSTOP=>1, MSG=>[ [1, sprintf("This is Bitflu %s running on Perl %vd",VERSION, $^V)] ]};
	}

	##########################################################################
	# Return version string
	sub _Command_Date {
		my($self) = @_;
		return {CHAINSTOP=>1, MSG=>[ [0, "".localtime()] ]};
	}
	##########################################################################
	# Return version string
	sub _Command_Sysinfo {
		my($self) = @_;
		
		my @A      = ();
		my $waste  = length(Data::Dumper::Dumper($self));
		my $uptime = $self->Network->GetTime - $self->{_BootTime};
		
		push(@A, [0, sprintf("Megabytes of memory wasted : %.3f",$waste/1024/1024)]);
		push(@A, [0, sprintf("Not crashed within %.3f minutes",($uptime/60))]);
		
		return {CHAINSTOP=>1, MSG=>\@A};
	}

	
	sub info  { my($self,$msg) = @_;  return if $self->Configuration->GetValue('loglevel') < 4;  print localtime()." # $msg\n"; }
	sub debug { my($self, $msg) = @_; return if $self->Configuration->GetValue('loglevel') < 10; print localtime()." # **  DEBUG  ** $msg\n"; }
	sub warn  { my($self,$msg) = @_;  return if $self->Configuration->GetValue('loglevel') < 2;  print localtime()." # ** WARNING ** $msg\n"; }
	sub stop  { my($self, $msg) = @_; print localtime()." # EXITING # $msg\n"; exit(1); }
	sub panic {
		my($self,$msg) = @_;
		$self->info("--------- BITFLU SOMEHOW MANAGED TO CRASH ITSELF; PANIC MESSAGE: ---------");
		$self->info($msg);
		$self->info("--------- BACKTRACE START ---------");
		Carp::cluck();
		$self->info("---------- BACKTRACE END ----------");
		
		$self->info("SHA1-Module used : ".$self->Sha1->{mname});
		$self->info("Perl Version     : ".sprintf("%vd", $^V));
		$self->info("Perl Execname    : ".$^X);
		$self->info("OS-Name          : ".$^O);
		$self->info("Running since    : ".gmtime($self->{_BootTime}));
		$self->info("##################################");
		exit(1);
	}
	
	
1;


####################################################################################################################################################
####################################################################################################################################################
# Bitflu Queue manager
#
package Bitflu::QueueMgr;
use constant SHALEN => 40;
	sub new {
		my($class, %args) = @_;
		my $self = {super=> $args{super}};
		bless($self,$class);
		return $self;
	}
	
	##########################################################################
	# Inits plugin: This resumes all found storage items
	sub init {
		my($self) = @_;
		my $queueIds = $self->{super}->Storage->GetStorageItems();
		my $runners  = $self->GetRunnersRef();
		
		foreach my $sid (@$queueIds) {
			$self->info("Resuming download $sid, this may take a few seconds...");
			my $this_storage = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage for sid $sid");
			my $owner = $this_storage->GetSetting('owner');
			if(defined($owner) && defined($runners->{$owner})) {
				$runners->{$owner}->resume_this($sid);
			}
			else {
				$self->panic("StorageObject $sid is owned by '$owner', but plugin is not loaded/registered correctly");
			}
		}
		$self->{super}->Admin->RegisterCommand('rename'  , $self, 'admincmd_rename', 'Renames a download',
		         [ [undef, "Renames a download"], [undef, "Usage: rename queue_id \"New Name\""] ]);
		$self->{super}->Admin->RegisterCommand('cancel'  , $self, 'admincmd_cancel', 'Removes a file from the download queue',
		         [ [undef, "Removes a file from the download queue"], [undef, "Usage: cancel queue_id [queue_id2 ...]"] ]);
		$self->info("--- startup completed: bitflu is ready ---");
		return 1;
	}
	
	##########################################################################
	# Cancel a queue item
	sub admincmd_cancel {
		my($self, @args) = @_;
		
		my $runners = $self->GetRunnersRef();
		my @A = ();
		
		if(int(@args)) {
			foreach my $cid (@args) {
				my $storage = $self->{super}->Storage->OpenStorage($cid);
				if($storage) {
					my $owner = $storage->GetSetting('owner');
					if(defined($owner) && defined($runners->{$owner})) {
						$runners->{$owner}->cancel_this($cid);
						push(@A, [1, "'$cid' canceled"]);
					}
					else {
					$self->panic("'$cid' has no owner, cannot cancel!");
					}
				}
				else {
					push(@A, [2, "'$cid' not removed from queue: No such item"]);
				}
			}
		}
		else {
			push(@A, [2, "Usage: cancel queue_id"]);
		}
		
		
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	##########################################################################
	# Rename a queue item
	sub admincmd_rename {
		my($self, @args) = @_;
		
		my $sha   = $args[0];
		my $nname = $args[1];
		
		if(!defined($nname)) {
			return({CHAINSTOP=>1, MSG=>[[undef,"Usage: rename queue_id newname"]]});
		}
		
		my $storage = $self->{super}->Storage->OpenStorage($sha);
		if(!$storage) {
			return({CHAINSTOP=>1, MSG=>[[2,"Unable to rename key $sha ; key does not exist in queue"]]});
		}
		else {
			$storage->SetSetting('name',$nname);
			return({CHAINSTOP=>1, MSG=>[[1,"Renamed $sha into $nname"]]});
		}
		$self->panic("NOTREACHED");
	}
	
	##########################################################################
	# Add a new item to queue (Also creates a new storage)
	sub AddItem {
		my($self, %args) = @_;
		
		my $name    = $args{Name};
		my $chunks  = $args{Chunks} or $self->panic("No chunks?!");
		my $size    = $args{Size};
		my $overst  = $args{Overshoot};
		my $flayout = $args{FileLayout} or $self->panic("FileLayout missing");
		my $shaname = ($args{ShaName} || unpack("H*", $self->{super}->Sha1->sha1($name)));
		my $owner   = ref($args{Owner}) or $self->panic("No owner?");
		
		if($size == 0 && $chunks != 1) {
			$self->panic("Sorry: You can not create a dynamic storage with multiple chunks ($chunks != 1)");
		}
		if(!defined($name)) {
			$self->panic("AddItem needs a name!");
		}
		if(length($shaname) != SHALEN) {
			$self->panic("Invalid shaname: $shaname");
		}
		
		
		my $sobj = $self->{super}->Storage->CreateStorage(StorageId => $shaname, Size=>$size, Chunks=>$chunks, Overshoot=>$overst, FileLayout=>$flayout);
		if($sobj) {
			$sobj->SetSetting('owner', $owner);
			$sobj->SetSetting('name' , $name);
			$sobj->SetSetting('createdat', $self->{super}->Network->GetTime);
		}
		else {
			$self->warn("Failed to create storage-object for $shaname");
		}
		return $sobj;
	}
	
	##########################################################################
	# Removes an item from the queue + storage
	sub RemoveItem {
		my($self,$sid) = @_;
		my $ret = $self->{super}->Storage->RemoveStorage($sid);
		if(!$ret) {
			$self->panic("Unable to remove storage-object $sid : $!");
		}
		
		delete($self->{statistics}->{$sid}) or $self->panic("Cannot remove non-existing statistics for $sid");
		return 1;
	}
	
	##########################################################################
	# Set private statistics
	# You are supposed to set total_bytes, total_chunks, done_bytes, done_chunks,
	#                     uploaded_bytes, clients, active_clients, last_recv
	# ..and we do not save anything.. you'll need to do this on your own :-)
	sub SetStats {
		my($self, $id, $ref) = @_;
		foreach my $xk (keys(%$ref)) {
			$self->{statistics}->{$id}->{$xk} = $ref->{$xk};
		}
	}
	
	sub IncrementStats {
		my($self, $id, $ref) = @_;
		foreach my $xk (keys(%$ref)) {
			$self->SetStats($id,{$xk => $self->GetStat($id,$xk)+$ref->{$xk}});
		}
	}
	sub DecrementStats {
		my($self, $id, $ref) = @_;
		foreach my $xk (keys(%$ref)) {
			$self->SetStats($id,{$xk => $self->GetStat($id,$xk)-$ref->{$xk}});
		}
	}
	
	##########################################################################
	# Get private statistics
	sub GetStats {
		my($self,$id) = @_;
		return $self->{statistics}->{$id};
	}
	
	##########################################################################
	# Get single statistics key
	sub GetStat {
		my($self,$id,$key) = @_;
		return $self->GetStats($id)->{$key};
	}
	
	##########################################################################
	# Returns a list with all queue objects
	sub GetQueueList {
		my($self) = @_;
		my $xh = ();
		my $all_ids = $self->{super}->Storage->GetStorageItems();
		foreach my $id (@$all_ids) {
			my $so = $self->{super}->Storage->OpenStorage($id) or $self->panic("Unable to open $id");
			my $name =  $so->GetSetting('name');
			my $type = ($so->GetSetting('type') or "????");
			$xh->{$type}->{$id} = { name=>$name };
		}
		return $xh;
	}
	
	
	##########################################################################
	# Returns a list of bitflus _Runner array as reference hash
	sub GetRunnersRef {
		my($self) = @_;
		my $runners = ();
		foreach my $r (@{$self->{super}->{_Runners}}) {
			$runners->{ref($r)} = $r;
		}
		return $runners;
	}
	
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }

1;

###############################################################################################################
# Bitflu SHA1-Module
package Bitflu::Sha1;
	
	##########################################################################
	# Create new object and try to load a module
	sub new {
		my($class, %args) = @_;
		my $self = { super => $args{super}, ns => '', mname => '' };
		bless($self,$class);
		
		foreach my $mname (qw(Digest::SHA Digest::SHA1)) {
			my $code = "use $mname; \$self->{ns} = $mname->new; \$self->{mname} = \$mname";
			eval $code;
		}
		
		if($self->{mname}) {
			$self->debug("Using $self->{mname}");
		}
		else {
			$self->stop("No SHA1-Module found. Bitflu requires 'Digest::SHA' (http://search.cpan.org)");
		}
		
		return $self;
	}
	
	sub init { return 1 }
	
	sub sha1_hex {
		my($self, $buff) = @_;
		$self->{ns}->add($buff);
		return $self->{ns}->hexdigest;
	}
	
	sub sha1 {
		my($self,$buff) = @_;
		$self->{ns}->add($buff);
		return $self->{ns}->digest;
	}
	
	sub debug  { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg);  }
	sub stop { my($self, $msg) = @_; $self->{super}->stop(ref($self).": ".$msg); }


1;


###############################################################################################################
# Bitflu Admin-Dispatcher : Release 20070319_1
package Bitflu::Admin;

	##########################################################################
	# Guess what?
	sub new {
		my($class, %args) = @_;
		my $self = {super=> $args{super}, cmdlist => {}, notifylist => {}};
		bless($self,$class);
		return $self;
	}
	
	##########################################################################
	# Init plugin
	sub init {
		my($self) = @_;
		$self->RegisterCommand("help",    $self, 'admincmd_help'   , 'Displays what you are reading now',
		 [ [undef, "Use 'help' to get a list of all commands"], [undef, "Type 'help command' to get help about 'command'"] ]);
		$self->RegisterCommand("plugins",  $self, 'admincmd_plugins', 'Displays all loaded plugins');
		$self->RegisterCommand("useradmin",$self, 'admincmd_useradm', 'Create and modify accounts',
		 [ [undef, "Usage: useradmin [set username password] [delete username] [list]"] ]);
		$self->RegisterNotify($self, 'receive_notify');
		return 1;
	}
	
	##########################################################################
	# Display registered plugins
	sub admincmd_plugins {
		my($self) = @_;
		
		my @A = ([1, "Hooks registered at bitflus NSFS (NotSoFairScheduler)"]);
		foreach my $r (@{$self->{super}->{_Runners}}) {
			push(@A,[undef,$r]);
		}
		
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	##########################################################################
	# Notification handler, we are just going to print them out using the logging
	sub receive_notify {
		my($self,$msg) = @_;
		$self->info("#NOTIFICATION#: $msg");
	}
	
	##########################################################################
	# BareBones help
	sub admincmd_help {
		my($self,$topic) = @_;
		my @A = ();
		
		if($topic) {
			if(defined($self->GetCommands->{$topic})) {
				my @instances = @{$self->GetCommands->{$topic}};
				
				foreach my $ci (@instances) {
					push(@A, [3, "Command '$topic' (Provided by plugin $ci->{class})"]);
					if($ci->{longhelp}) {
						push(@A, @{$ci->{longhelp}});
					}
					else {
						push(@A, [undef, $ci->{help}]);
					}
					push(@A, [undef, '']);
				}
			}
			else {
				push(@A, [2, "No help for '$topic', command does not exist"]);
			}
		}
		else {
			foreach my $xcmd (sort (keys %{$self->GetCommands})) {
				my $lb = sprintf("%-20s", $xcmd);
				my @hlps = ();
				foreach my $instance (@{$self->GetCommands->{$xcmd}}) {
					push(@hlps, "$instance->{help}");;
				}
				
				$lb .= join(' / ',@hlps);
				
				push(@A, [undef, $lb]);
			}
		}
		
		
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	##########################################################################
	# Handles useradm commands
	sub admincmd_useradm {
		my($self, @args) = @_;
		my @A = ();
		
		my($cmd,$usr,$pass) = @args;
		
		if($cmd eq 'set' && $pass) {
			$self->__useradm_modify(Inject => {User=>$usr, Pass=>$pass});
			push(@A, [1, "Useraccount updated"]);
		}
		elsif($cmd eq 'delete' && $usr) {
			if(defined $self->__useradm_modify->{$usr}) {
				# -> Account exists
				$self->__useradm_modify(Drop => {User=>$usr});
				push(@A, [1, "Account '$usr' removed"]);
				$self->panic("BUG") if (defined $self->__useradm_modify->{$usr}); # Paranoia check
			}
			else {
				push(@A, [2, "Account '$usr' does not exist"]);
			}
		}
		elsif($cmd eq 'list') {
			push(@A, [3, "Configured accounts:"]);
			foreach my $k (keys(%{$self->__useradm_modify})) {
				push(@A, [undef,$k]);
			}
		}
		else {
			push(@A, [2, "Type 'help useradmin' for more information"]);
		}
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	##########################################################################
	# Create password entry
	sub __useradm_mkentry {
		my($self,$usr,$pass) = @_;
		$usr =~ tr/: ;=//d;
		return undef if length($usr) == 0;
		return $usr.":".$self->{super}->Sha1->sha1_hex("$usr;$pass");
	}
	
	##########################################################################
	# Modify current setting
	sub __useradm_modify {
		my($self,%args) = @_;
		my @result    = ();
		my $allusr    = {};
		my $to_inject = '';
		my $delta     = 0;
		foreach my $entry (split(/;/,$self->{super}->Configuration->GetValue('useradm'))) {
			if(my($user,$hash) = $entry =~ /^([^:]*):(.+)$/) {
				if ($user ne $args{Inject}->{User} && $user ne $args{Drop}->{User}) {
					push(@result,$entry);
				}
				else {
					$delta++;
				}
				$allusr->{$user} = $entry;
			}
			else {
				$self->warn("Useradmin: Wiping garbage entry: '$entry'");
			}
		}
		
		if(exists($args{Inject}->{User})) {
			$to_inject = $args{Inject}->{User};
			$to_inject =~ tr/:; //d;
			$delta++;
		}
		
		if(length($to_inject) > 0) {
			push(@result,$self->__useradm_mkentry($to_inject,$args{Inject}->{Pass}));
		}
		
		$self->{super}->Configuration->SetValue('useradm', join(';', @result)) if $delta;
		return $allusr;
	}
	
	
	##########################################################################
	# Register Notify handler
	sub RegisterNotify {
		my($self, $xref, $xcmd) = @_;
		$self->debug("RegisterNotify: Will notify $xref via $xref->$xcmd");
		$self->{notifylist}->{$xref} = { class => $xref, cmd => $xcmd };
	}
	
	##########################################################################
	# Send out notifications
	sub SendNotify {
		my($self,$msg) = @_;
		foreach my $kx (keys(%{$self->{notifylist}})) {
			my $nc = $self->{notifylist}->{$kx};
			my $class = $nc->{class}; my $cmd = $nc->{cmd};
			$class->$cmd($msg);
		}
	}
	
	##########################################################################
	# Registers a new command to be used with ExecuteCommand
	sub RegisterCommand {
		my($self,$name,$xref,$xcmd,$helptext,$longhelp) = @_;
		$self->debug("RegisterCommand: Hooking $name to $xref->$xcmd");
		push(@{$self->{cmdlist}->{$name}}, {class=>$xref, cmd=>$xcmd, help=>$helptext, longhelp=>$longhelp});
		$helptext or $self->panic("=> $xcmd ; $xref");
	}
	
	##########################################################################
	# Returns the full cmdlist
	sub GetCommands {
		my($self) = @_;
		return $self->{cmdlist};
	}
	
	##########################################################################
	# Execute a command!
	sub ExecuteCommand {
		my($self,$command,@args) = @_;
		my $plugin_hits = 0;
		my @plugin_msg = ();
		
		if(ref($self->GetCommands->{$command}) eq "ARRAY") {
			foreach my $ref (@{$self->GetCommands->{$command}}) {
				my $class = $ref->{class};
				my $cmd   = $ref->{cmd};
				my $bref = $class->$cmd(@args);
				if($bref->{CHAINSTOP}) {
					return($bref);
				}
				foreach my $al (@{$bref->{MSG}}) {
					push(@plugin_msg, [undef, $al->[1]]);
				}
				$plugin_hits++;
			}
		}
		
		if($plugin_hits == 0) {
			push(@plugin_msg, [undef, "Unknown command '$command'"]);
		}
		else {
			unshift(@plugin_msg, [undef, "All plugins failed to execute '$command @args'"]);
		}
		
		return({CHAINSTOP=>0, MSG=>\@plugin_msg});
	}
	
	##########################################################################
	# Returns TRUE if Authentication was successful (or disabled (= no accounts))
	sub AuthenticateUser {
		my($self,%args) = @_;
		my $numentry = int(keys(%{$self->__useradm_modify}));
		return 1 if $numentry == 0; # No users, no security
		
		my $expect = $self->__useradm_mkentry($args{User}, $args{Pass});
		if(defined($expect) && $self->__useradm_modify->{$args{User}} eq $expect ) {
			return 1;
		}
		else {
			return 0;
		}
	}
	
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }

1;


###############################################################################################################
# Bitflu Network-IO Lib : Release 20071220_1
package Bitflu::Network;

use strict;
use IO::Socket;
use IO::Select;
use POSIX;

use constant NETSTATS     => 3;             # ReGen netstats each 3 seconds
use constant MAXONWIRE    => 1024*1024;     # Do not buffer more than 1mb per client connection
use constant BPS_MIN      => 8;             # Minimal upload speed per socket
use constant DEVNULL      => '/dev/null';   # Path to /dev/null
use constant LT_UDP       => 1;             # Internal ID for UDP sockets
use constant LT_TCP       => 2;             # Internal ID for TCP sockets

	##########################################################################
	# Creates a new Networking Object
	sub new {
		my($class, %args) = @_;
		my $self = {super=> $args{super}, bpc=>BPS_MIN, NOWTIME => undef , _bitflu_network => {}, avfds => 0,
		            stats => {nextrun=>0, sent=>0, recv=>0, raw_recv=>0, raw_sent=>0} };
		bless($self,$class);
		$self->SetTime;
		$self->{avfds} = $self->TestFileDescriptors;
		$self->debug("Reserved $self->{avfds} file descriptors for networking");
		return $self;
	}
	
	##########################################################################
	# Register Admin commands
	sub init {
		my($self) = @_;
		$self->{super}->Admin->RegisterCommand('netstat'  , $self, '_Command_Netstat', 'Displays networking information');
		$self->SetTime;
		return 1;
	}
	
	##########################################################################
	# Display netstat command
	sub _Command_Netstat {
		my($self) = @_;
		my @A = ();
		my $bfn  = $self->{_bitflu_network};
		
		push(@A, [3, "Total file descriptors left : $self->{avfds}"]);
		
		foreach my $item (keys(%$bfn)) {
			if(defined($bfn->{$item}->{config})) {
				push(@A, [4, '-------------------------------------------------------------------------']);
				push(@A, [1, "Handle: $item"]);
				push(@A, [undef,"Active connections              : $bfn->{$item}->{config}->{cntMaxPeers}"]);
				push(@A, [undef,"Connection hardlimit            : $bfn->{$item}->{config}->{MaxPeers}"]);
				push(@A, [undef,"Connections not yet established : ".int(keys(%{$bfn->{$item}->{establishing}}))]);
			}
		}
		
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	##########################################################################
	# Test how many filedescriptors this OS / env can handle
	sub TestFileDescriptors {
		my($self)   = @_;
		my $i       = 0;
		my @fdx     = ();
		my $sysr    = 0xF;
		my $canhave = 0;
		
		open(FAKE, DEVNULL) or $self->stop("Unable to open ".DEVNULL.": $!");
		close(FAKE);
		
		while($i++ < 2048) {
			unless( open($fdx[$i], DEVNULL) ) {
				last;
			}
		}
		if($i > $sysr) {
			$canhave = $i - $sysr;
		}
		else {
			$self->panic("Sorry, bitfu can not run with only $i filedescriptors left");
		}
		while(--$i > 0) {
			close($fdx[$i]) or $self->panic("Unable to close TestFD # $i : $!");
		}
		return $canhave;
	}
	
	##########################################################################
	# Refresh buffered time
	sub SetTime {
		my($self) = @_;
		$self->{NOWTIME} = time();
	}
	
	##########################################################################
	# Returns buffered time
	sub GetTime {
		my($self) = @_;
		return $self->{NOWTIME};
	}
	
	##########################################################################
	# Returns bandwidth statistics
	sub GetStats {
		my($self) = @_;
		return $self->{stats};
	}
	
	##########################################################################
	# Returns last IO for given socket
	sub GetLastIO {
		my($self,$socket) = @_;
		$self->panic("Cannot return lastio of vanished socket <$socket>") unless exists($self->{_bitflu_network}->{$socket});
		return $self->{_bitflu_network}->{$socket}->{lastio};
	}
	
	##########################################################################
	# Returns TRUE if socket is an INCOMING connection
	sub IsIncoming {
		my($self,$socket) = @_;
		my $val = $self->{_bitflu_network}->{$socket}->{incoming};
		$self->panic("$socket has an undef value for 'incoming'") unless defined($val);
		return $val;
	}
	
	##########################################################################
	# Create an UDP-Listen socket
	sub NewUdpListen {
		my($self,%args) = @_;
		return undef if(!defined($args{ID}));
		return undef if(!defined($args{Port}));
		
		if(exists($self->{_bitflu_network}->{$args{ID}})) {
			$self->panic("FATAL: $args{ID} has a listening socket, unable to create a second instance with the same ID");
		}
		
		my $new_socket = IO::Socket::INET->new(LocalPort=>$args{Port}, LocalAddr=>$args{Bind}, Proto=>'udp') or return undef;
		
		$self->{_bitflu_network}->{$args{ID}}               = { select => undef, socket => $new_socket, rqi => 0, wqi => 0, config => { MaxPeers=>1, cntMaxPeers=>0, } };
		$self->{_bitflu_network}->{$args{ID}}->{listentype} = LT_UDP;
		$self->{_bitflu_network}->{$args{ID}}->{select}     = new IO::Select   or $self->panic("Unable to create new IO::Select object: $!");
		$self->{_bitflu_network}->{$args{ID}}->{select}->add($new_socket)      or $self->panic("Unable to glue <$new_socket> to select object of $args{ID}: $!");
		$self->{_bitflu_network}->{$args{ID}}->{callbacks}  = $args{Callbacks} or $self->panic("Unable to register UDP-Socket without any callbacks");
		$self->Unblock($new_socket) or $self->panic("Unable to unblock $new_socket");
		return $new_socket;
	}
	
	##########################################################################
	# Try to create a new listening socket
	# NewTcpListen(ID=>UniqueueRunnerId, Port=>PortToListen, Bind=>IPv4ToBind, Callbacks => {})
	sub NewTcpListen {
		my($self,%args) = @_;
		return undef if(!defined($args{ID}));
		my $socket = 0;
		
		if(exists($self->{_bitflu_network}->{$args{ID}})) {
			$self->panic("FATAL: $args{ID} has a listening socket, unable to create a second instance with the same ID");
		}
		elsif($args{MaxPeers} < 1) {
			$self->panic("$args{ID}: cannot reserve '$args{MaxPeers}' file descriptors");
		}
		elsif($args{Port}) {
			$socket = IO::Socket::INET->new(LocalPort=>$args{Port}, LocalAddr=>$args{Bind}, Proto=>'tcp', ReuseAddr=>1, Listen=>1) or return undef;
		}
		
		$self->{_bitflu_network}->{$args{ID}} = { select => undef, socket => $socket,  rqi => 0, wqi => 0, config => { MaxPeers=>($args{MaxPeers}), cntMaxPeers=>0 } };
		$self->{_bitflu_network}->{$args{ID}}->{select}     = new IO::Select or $self->panic("Unable to create new IO::Select object: $!");
		$self->{_bitflu_network}->{$args{ID}}->{listentype} = LT_TCP;
		$self->{_bitflu_network}->{$args{ID}}->{callbacks}  = $args{Callbacks} or $self->panic("Unable to register TCP-Socket without any callbacks");
		
		if($socket) {
			$self->{_bitflu_network}->{$args{ID}}->{select}->add($socket) or $self->panic("Unable to glue <$socket> to select object of $args{ID}: $!");
		}
		
		return $socket;
	}
	
	
	##########################################################################
	# Creates a new (outgoing) connection
	# NewTcpConnection(ID=>UniqueRunnerId, Ipv4=>Ipv4, Port=>PortToConnect);
	sub NewTcpConnection {
		my($self, %args) = @_;
		return undef if(!defined($args{ID}));
		
		my $bfn_strct = $self->{_bitflu_network}->{$args{ID}};
		
		if(--$self->{avfds} < 0) {
			$self->{avfds}++;
			return undef;
		}
		elsif(++$bfn_strct->{config}->{cntMaxPeers} > $bfn_strct->{config}->{MaxPeers}) {
			$bfn_strct->{config}->{cntMaxPeers}--;
			$self->{avfds}++;
			return undef;
		}
		elsif($bfn_strct->{listentype} != LT_TCP) {
			$self->panic("Cannot create TCP connection for socket of type ".$bfn_strct->{listentype}." using $args{ID}");
		}
		
		# This shouldn't be needed anymore
		if(!defined($bfn_strct->{select})) {
			$self->panic("No select option, depricated code has been used (FIXME / REMOVEME)");
		}
		
		
		my $proto = getprotobyname('tcp');
		my $sock  = undef;
		my $sin = undef;
		socket($sock, AF_INET,SOCK_STREAM,$proto) or $self->panic("Failed to create a new socket : $!");
		eval { $sin = sockaddr_in($args{Port}, inet_aton($args{Ipv4})); };
		
		if(!defined($sin)) {
			$self->warn("Unable to create socket for $args{Ipv4}:$args{Port}");
			$bfn_strct->{config}->{cntMaxPeers}--;
			$self->{avfds}++;
			return undef;
		}
		
		
		$self->Unblock($sock) or $self->panic("Failed to unblock new socket <$sock> : $!");
		if(exists($self->{_bitflu_network}->{$sock})) {
			$self->panic("FATAL: DUPLICATE SOCKET-ID <$sock> ?!");
		}
		
		# Write PerSocket information: establishing | outbuff | config
		$bfn_strct->{establishing}->{$sock} = { socket => $sock, till => $self->GetTime+$args{Timeout}, sin => $sin };
		$self->{_bitflu_network}->{$sock}   = { sockmap => $sock, handlemap => $args{ID}, fastwrite => 0, lastio => $self->GetTime, incoming => 0 };
		return $sock;
	}
	
	
	##########################################################################
	# Run Network IO
	# Run(UniqueIdToRun,{callbacks});
	sub Run {
		my($self, $handle_id) = @_;
		my $select_handle = $self->{_bitflu_network}->{$handle_id}->{select} or $self->panic("$handle_id has no select handle");
		my $callbacks     = $self->{_bitflu_network}->{$handle_id}->{callbacks};
		$self->SetTime;
		$self->_Throttle;
		$self->_Establish($handle_id, $callbacks, $select_handle);
		$self->_IOread($handle_id, $callbacks, $select_handle);
		$self->_IOwrite($handle_id,$callbacks, $select_handle);
	}
	
	##########################################################################
	# Check establishing-queue
	sub _Establish {
		my($self, $handle_id, $callbacks, $select_handle) = @_;
		foreach my $ref (values(%{$self->{_bitflu_network}->{$handle_id}->{establishing}})) {
			connect($ref->{socket},$ref->{sin});
			if($!{'EISCONN'}) {
				delete($self->{_bitflu_network}->{$handle_id}->{establishing}->{$ref->{socket}});
				$select_handle->add($ref->{socket});
			}
			elsif($ref->{till} < $self->GetTime) {
				if(my $cbn = $callbacks->{Close}) { $handle_id->$cbn($ref->{socket}); }
				$self->{_bitflu_network}->{$handle_id}->{config}->{cntMaxPeers}--;
				$self->{avfds}++;
				delete($self->{_bitflu_network}->{$handle_id}->{establishing}->{$ref->{socket}})  or $self->panic("Cannot remove ".$ref->{socket}." from $handle_id");
				delete($self->{_bitflu_network}->{$ref->{socket}})                                or $self->panic("Cannot remove ".$ref->{socket});
				delete($self->{_bitflu_network}->{$handle_id}->{writeq}->{$ref->{socket}});
				close($ref->{socket});
			}
		}
	}
	
	##########################################################################
	# Read from a bunch of sockets
	sub _IOread {
		my($self, $handle_id, $callbacks, $select_handle) = @_;
		
		
		if($self->{_bitflu_network}->{$handle_id}->{rqi} == 0) {
			# Refill cache
			my @sq = $select_handle->can_read(0);
			$self->{_bitflu_network}->{$handle_id}->{rq} = \@sq;
			$self->{_bitflu_network}->{$handle_id}->{rqi} = int(@sq);
		}
		
		my $rpr = $self->{super}->Configuration->GetValue('readpriority');
		
		while($self->{_bitflu_network}->{$handle_id}->{rqi} > 0) {
			my $handle_ref = $self->{_bitflu_network}->{$handle_id};       # Get HandleID structure
			my $tor        = --$handle_ref->{rqi};                         # Current Index to Read
			my $socket     = ${$handle_ref->{rq}}[$tor] or $self->panic(); # Current Socket
			
			if($socket eq $handle_ref->{socket} && $handle_ref->{listentype} == LT_TCP) {
				my $new_sock = $socket->accept();
				my $new_ip   = '';
				if(!defined($new_sock)) {
					$self->info("Unable to accept new socket <$new_sock> : $!");
				}
				elsif(--$self->{avfds} < 0) {
					$self->warn("System has no file-descriptors left, dropping new incoming connection");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
					$self->{avfds}++;
				}
				elsif(!$self->Unblock($new_sock)) {
					$self->info("Unable to unblock $new_sock : $!");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
					$self->{avfds}++;
				}
				elsif(!($new_ip = $new_sock->peerhost)) {
					$self->debug("Unable to obtain peerhost from $new_sock : $!");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
					$self->{avfds}++;
				}
				elsif(++$handle_ref->{config}->{cntMaxPeers} > $handle_ref->{config}->{MaxPeers}) {
					$self->warn("Handle <$handle_id> is full: Dropping new socket");
					$handle_ref->{config}->{cntMaxPeers}--;
					$self->{avfds}++;
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
				}
				else {
					$self->{_bitflu_network}->{$new_sock} = { sockmap => $new_sock, handlemap => $handle_id, fastwrite => 0, lastio => $self->GetTime, incoming => 1 };
					$select_handle->add($new_sock);
					if(my $cbn = $callbacks->{Accept}) { $handle_id->$cbn($new_sock,$new_ip); }
				}
			}
			elsif(exists($self->{_bitflu_network}->{$socket})) {
				my $full_buffer  = '';
				my $full_bufflen = 0;
				my $last_bufflen = 0;
				
				for(0..8) {
					my $pb        = '';
					$last_bufflen = ( read($socket,$pb,POSIX::BUFSIZ) || 0 ); # Removes warnings ;-)
					$full_buffer  .= $pb;
					$full_bufflen += $last_bufflen;
					last if $last_bufflen != POSIX::BUFSIZ;
				}
				
				if($full_bufflen != 0) {
					# We read 'something'. If there was an error, we'll pick it up next time
					$self->{stats}->{raw_recv}                   += $full_bufflen;
					$self->{_bitflu_network}->{$socket}->{lastio} = $self->GetTime;
					if(my $cbn = $callbacks->{Data}) { $handle_id->$cbn($socket, \$full_buffer, $full_bufflen); }
				}
				else {
					if(my $cbn = $callbacks->{Close}) { $handle_id->$cbn($socket); }
					$self->RemoveSocket($handle_id,$socket);
				}
			}
			elsif($handle_ref->{listentype} == LT_UDP) {
				my $buffer = undef;
				$socket->recv($buffer,POSIX::BUFSIZ);
				if(my $cbn = $callbacks->{Data}) { $handle_id->$cbn($socket, \$buffer); }
			}
			else {
				$self->warn("Skipping read from <$socket> / Not active?");
			}
			last if --$rpr < 0;
		}
	}
	
	##########################################################################
	# Write to some sockets
	sub _IOwrite {
		my($self, $handle_id, $callbacks, $select_handle) = @_;
		
		my $handle_ref = $self->{_bitflu_network}->{$handle_id};
		
		if($handle_ref->{wqi} == 0) {
			# Refill cache
			my @sq = (values(%{$handle_ref->{writeq}}));
			$handle_ref->{wq} = \@sq;
			$handle_ref->{wqi} = int(@sq);
		}
		
		my $wpr = $self->{super}->Configuration->GetValue('writepriority');
		
		while($handle_ref->{wqi} > 0) {
			my $tow    = --$handle_ref->{wqi};
			my $socket = ${$handle_ref->{wq}}[$tow];
			$self->panic("No socket!") unless $socket;
			next unless exists($self->{_bitflu_network}->{$handle_id}->{writeq}->{$socket}); # Socket vanished or no writequeue
			$self->_TryWrite(Socket=>$socket, Handle=>$handle_id, CanKill=>1);
			last if --$wpr < 0;
		}
	}
	
	
	##########################################################################
	# Try to write data to a socket
	sub _TryWrite {
		my($self, %args) = @_;
		
		my $socket        = $args{Socket}                               or $self->panic;
		my $handle_id     = $args{Handle}                               or $self->panic;
		my $socket_strct  = $self->{_bitflu_network}->{$socket}         or $self->panic;
		my $handle_strct  = $self->{_bitflu_network}->{$args{Handle}}   or $self->panic;
		my $select_handle = $handle_strct->{select}                     or $self->panic;
		my $bufsize       = ($self->{bpc} + $socket_strct->{fastwrite}) or $self->panic;
		my $cankill       = $args{CanKill};
		
		if(!$select_handle->exists($socket)) { return; } # not yet connected
		
		my $bytes_sent    = syswrite($socket, $socket_strct->{outbuff}, ($bufsize > (POSIX::BUFSIZ) ? (POSIX::BUFSIZ) : $bufsize) );
		
		if($!{'EISCONN'}) {
			#$self->debug("EISCONN returned.");
		}
		elsif(!defined($bytes_sent)) {
			if($!{'EAGAIN'} or $!{'EWOULDBLOCK'}) {
				#$self->warn("$wsocket returned EAGAIN");
			}
			elsif($cankill) {
				if(my $cbn = $handle_strct->{callbacks}->{Close}) { $handle_id->$cbn($socket); }
				$self->RemoveSocket($handle_id,$socket);
			}
			else {
				$self->warn("Delaying kill of $handle_id -> $socket [Write failed with: $!]");
			}
		}
		else {
			$self->{stats}->{raw_sent} += $bytes_sent;
			$socket_strct->{qlen}      -= $bytes_sent;
			$socket_strct->{outbuff}    = substr($socket_strct->{outbuff},$bytes_sent);
			$socket_strct->{fastwrite}  = 0 if( ($socket_strct->{fastwrite} -= $bytes_sent) < 0);
			if($socket_strct->{qlen} == 0) {
				delete($handle_strct->{writeq}->{$socket}) or $self->panic("Deleting non-existing socket: Handle: $handle_id ; Sock: $socket");
			}
		}
	}

	
	##########################################################################
	# Calculate new value for bcp
	sub _Throttle {
		my($self) = @_;
		return if $self->GetTime <= $self->{stats}->{nextrun};
		my $UPSPEED = $self->{super}->Configuration->GetValue('upspeed') * 1024;
		
		if($self->{stats}->{nextrun} != 0) {
			my $resolution = $self->GetTime - $self->{stats}->{nextrun} + NETSTATS;
			$self->{stats}->{sent} = $self->{stats}->{raw_sent} / $resolution;
			$self->{stats}->{recv} = $self->{stats}->{raw_recv} / $resolution;
			$self->{stats}->{raw_sent} = 0;
			$self->{stats}->{raw_recv} = 0;
			# Throttle upspeed
			my $current_upspeed = $self->{stats}->{sent};
			my $wanted_upspeed  = $UPSPEED;
			my $upspeed_drift   = $UPSPEED-$current_upspeed;
			my $upspeed_adjust  = 1; # = Nothing
			
			if($upspeed_drift < -500) {
				$upspeed_adjust = ($wanted_upspeed/($current_upspeed+1));
			}
			elsif($upspeed_drift > 500) {
				$upspeed_adjust = ($wanted_upspeed/($current_upspeed+1));
			}
			
			$upspeed_adjust = 1.3 if $upspeed_adjust > 1.3; # Do not bump up too fast..
			$self->{bpc} = int($self->{bpc} * $upspeed_adjust);
			if($self->{bpc} < BPS_MIN)          { $self->{bpc} = BPS_MIN }
			elsif($self->{bpc} > POSIX::BUFSIZ) { $self->{bpc} = POSIX::BUFSIZ }
		}
		
		$self->{stats}->{nextrun} = NETSTATS + $self->GetTime;
	}
	
	
	
	##########################################################################
	# Remove socket
	# RemoveSocket(UniqueRunId, Socket)
	sub RemoveSocket {
		my($self,$handle_id, $socket) = @_;
		
		if($self->{_bitflu_network}->{$handle_id}->{select}->exists($socket)) {
			$self->{_bitflu_network}->{$handle_id}->{select}->remove($socket) or $self->panic("Unable to remove <$socket>");
		}
		elsif(delete($self->{_bitflu_network}->{$handle_id}->{establishing}->{$socket})) {
			# Kill unestablished sock
		}
		else {
			$self->panic("FATAL: <$socket> was not attached to IO::Select and not establishing!");
		}
		
		# Correct statistics
		$self->{_bitflu_network}->{$handle_id}->{config}->{cntMaxPeers}--;
		$self->{avfds}++;
		
		# Wipe socket itself + writeq
		delete($self->{_bitflu_network}->{$socket}) or $self->panic("Unable to remove non-existent socketmap for <$socket>");
		delete($self->{_bitflu_network}->{$handle_id}->{writeq}->{$socket});
		close($socket) or $self->panic("Unable to close socket $socket : $!");
	}
	
	
	##########################################################################
	# Send UPD datagram
	sub SendUdp {
		my($self, $socket, %args) = @_;
		my $ip   = $args{Ip}   or $self->panic("No IP given");
		my $port = $args{Port} or $self->panic("No Port given");
		my $data = $args{Data};
		my $hisip = IO::Socket::inet_aton($ip);
		my $hispn = IO::Socket::sockaddr_in($port, $hisip);
		my $bs = send($socket,$data,0,$hispn);
	}
	
	##########################################################################
	# FastWrite data
	sub WriteDataNow {
		my($self,$socket,$buffer) = @_;
		$self->{_bitflu_network}->{$socket}->{fastwrite} += length($buffer);
		$self->WriteData($socket,$buffer);
	}
	
	##########################################################################
	# Write Data to socket
	# WriteData(UniqueRunId, Socket, DataToWRite)
	sub WriteData {
		my($self, $socket, $buffer) = @_;
		
		my $gotspace     = 1;
		my $queued_bytes = ($self->{_bitflu_network}->{$socket}->{qlen} or 0);
		my $this_bytes   = length($buffer);
		my $total_bytes  = $queued_bytes + $this_bytes;
		my $handle_id    = $self->{_bitflu_network}->{$socket}->{handlemap} or $self->panic("No handleid for $socket ?");
		
		if($total_bytes > MAXONWIRE) {
			$self->warn("<$socket> Buffer overflow! Too much unsent data: $total_bytes bytes");
			$gotspace = 0;
		}
		elsif($self->{_bitflu_network}->{$handle_id}->{listentype} != LT_TCP) {
			$self->panic("Cannot write tcp data to non-tcp socket $socket ($self->{_bitflu_network}->{$handle_id}->{listentype})");
		}
		else {
			$self->{_bitflu_network}->{$socket}->{outbuff}              .= $buffer;
			$self->{_bitflu_network}->{$socket}->{lastio}                = $self->GetTime;
			$self->{_bitflu_network}->{$socket}->{qlen}                  = $total_bytes;
			$self->{_bitflu_network}->{$handle_id}->{writeq}->{$socket}  = $socket; # Triggers a new write
			
			if($self->{_bitflu_network}->{$socket}->{fastwrite}) {
				$self->_TryWrite(Socket=>$socket,Handle=>$handle_id, CanKill=>0);
			}
		}
		return $gotspace;
	}
	
	
	
	##########################################################################
	# Set O_NONBLOCK on socket
	sub Unblock {
		my($self, $cfh) = @_;
		$self->panic("No filehandle given") unless $cfh;
		my $flags = fcntl($cfh, F_GETFL, 0)
								or return undef;
		fcntl($cfh, F_SETFL, $flags | O_NONBLOCK)
		or return undef;
		return 1;
	}

	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
	sub stop  { my($self, $msg) = @_; $self->{super}->stop(ref($self).": ".$msg); }

	
1;


###############################################################################################################
# Builtin Configuration parser / class
package Bitflu::Configuration;
use strict;


	sub new {
		my($class, %args) = @_;
		my $self = { configuration_file => $args{configuration_file}, super=> $args{super} };
		bless($self, $class);
		
		unless(-f $self->{configuration_file}) {
			warn("-> Creating configuration file '$self->{configuration_file}'");
			open(CFGH, ">", $self->{configuration_file}) or die("Unable to create $self->{configuration_file}: $!\n");
			close(CFGH);
		}
		
		if (-f $self->{configuration_file}) {
			open(CFGH, "+<", $self->{configuration_file}) or die("Unable to open $self->{configuration_file} for writing: $!\n");
			$self->{configuration_fh} = *CFGH;
		}
		
		# Load the configuration ASAP to get logging working:
		$self->Load;
		return $self;
	}
	
	sub init {
		my($self) = @_;
		$self->{super}->Admin->RegisterCommand('config', $self, '_Command_config', 'Configure bitflu while running. Type \'help config\' for more information',
		[ [undef, "Usage: config show|get key|set key"],
		  [undef, "config show          : Display contents of .bitflu.config"],
		  [undef, "config get key       : Display value of 'key'. Example: 'config get upspeed'"],
			[undef, "config set key foo   : Changes value of 'key' to 'foo'. Example: 'config set upspeed 45'"],
			[undef, ""],
			[1, "NOTE: Certain options (like telnet_port) would require a restart of $0 and cannot be changed"],
			[1, "      using the 'config' command. To edit such options you'll need to stop bitflu and edit .bitflu.config"],
			[1, "      using a text editor."],
		]
		);
		return 1;
	}
	
	sub _Command_config {
		my($self,@args) = @_;
		my $msg    = undef;
		my $action = $args[0];
		my $key    = $args[1];
		my $value  = $args[2];
		my @A      = ();
		if($action eq "show") {
			foreach my $k (sort keys(%{$self->{conf}})) {
				push(@A, [undef, sprintf("%-20s => %s",$k, $self->{conf}->{$k})]);
			}
		}
		elsif($action eq "get" && defined($key)) {
			my $xval = $self->GetValue($key);
			if(defined($xval)) {
				push(@A, [undef, "$key => $xval"]);;
			}
			else {
				push(@A, [2, "$key is not set"]);
			}
		}
		elsif($action eq "set" && defined($value)) {
			if(defined($self->GetValue($key))) {
				if($self->SetValue($key, $value)) { push(@A, [undef, "$key set to $value"]); $self->Save; }
				else                              { push(@A, [2, "Unable to change value of $key at runtime"]); }
			}
			else {
				push(@A, [2, "Option '$key' does not exist"]);
			}
		}
		else {
			push(@A, [undef, "Type 'help config' for more information"]);
		}
		return{CHAINSTOP=>1, MSG=>\@A};
	}
	
	
	sub Load {
		my($self) = @_;
		$self->SetDefaults();
		if(defined(my $cfh = $self->{configuration_fh})) {
			seek($cfh,0,0) or $self->panic("Unable to seek to beginning");
			while(<$cfh>) {
				my $line = $_; chomp($line);
				if($line =~ /^#/ or $line =~ /^\s*$/) {
					next; # Comment or empty line
				}
				elsif($line =~ /^([a-zA-Z_]+)\s*=\s*(.*)$/) {
					$self->{conf}->{$1} = $2;
				}
				else {
					$self->panic("Error while parsing $self->{configuration_file}, syntax error on line $. : $line");
				}
			}
		}
	}
	
	sub SetDefaults {
		my($self) = @_;
		$self->{conf}->{plugindir}       = './plugins';
		$self->{conf}->{workdir}         = "./workdir";
		$self->{conf}->{incompletedir}   = "downloading";
		$self->{conf}->{completedir}     = "committed";
		$self->{conf}->{tempdir}         = "tmp";
		$self->{conf}->{upspeed}         = 35;
		$self->{conf}->{writepriority}   = 2;
		$self->{conf}->{readpriority}    = 4;
		$self->{conf}->{loglevel}        = 5;
		$self->{conf}->{renice}          = 8;
		$self->{conf}->{sleeper}         = 0.06;
		foreach my $opt qw(renice plugindir workdir incompletedir completedir tempdir) {
			$self->RuntimeLockValue($opt);
		}
	}
	
	sub Save {
		my($self) = @_;
		my $cfh = $self->{configuration_fh} or return undef;
		seek($cfh,0,0)   or $self->panic("Unable to seek to beginning");
		truncate($cfh,0) or $self->panic("Unable to truncate configuration file");
		print $cfh "# Configuration written by $0 (PID: $$) at ".localtime()."\n\n";
		foreach my $key (sort(keys(%{$self->{conf}}))) {
			print $cfh sprintf("%-20s = %s\n", $key, $self->{conf}->{$key});
		}
		print $cfh "\n# EOF #\n";
		$cfh->autoflush(1);
	}
	
	sub RuntimeLockValue {
		my($self,$xkey) = @_;
		return($self->{conf_setlock}->{$xkey} = 1);
	}
	
	sub IsRuntimeLocked {
		my($self, $xkey) = @_;
		return(defined($self->{conf_setlock}->{$xkey}));
	}
	
	sub GetValue {
		my($self,$xkey) = @_;
		return($self->{conf}->{$xkey});
	}
	
	sub SetValue {
		my($self,$xkey,$xval) = @_;
		return undef if defined($self->{conf_setlock}->{$xkey});
		$self->{conf}->{$xkey} = $xval;
		$self->Save;
		return 1;
	}
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
	
1;



