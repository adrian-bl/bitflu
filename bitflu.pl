#!/usr/bin/perl -w
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use Data::Dumper;


my $bitflu_run           = undef;     # Start as not_running and not_killed
$SIG{PIPE}  = $SIG{CHLD} = 'IGNORE';
$SIG{INT}   = $SIG{HUP}  = $SIG{TERM} = \&HandleShutdown;

# -> Create bitflu object
my $bitflu = Bitflu->new(configuration_file=>'.bitflu.config') or Carp::confess("Unable to create Bitflu Object");
$bitflu->LoadPlugins('Bitflu');
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

$bitflu->info("-> Shutdown completed");
exit(0);


sub HandleShutdown {
	my($sig) = @_;
	if(defined($bitflu_run) && $bitflu_run == 1) {
		# $bitflu is running, so we can use ->info
		$bitflu->info("-> Starting shutdown... (signal $sig received)");
	}
	$bitflu_run = 0; # set it to not_running and killed
}

package Bitflu;
use strict;
use Carp;
use constant VERSION => "20070823";

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
		
		opendir(PLUGINS, $pdirpath) or $self->abort("Unable to read directory '$pdirpath' : $!");
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
				$self->abort(" -> Please fix or remove this broken plugin file from $pdirpath");
			}
		}
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
		my $uid    = int($self->Configuration->GetValue('runas_uid'));
		my $gid    = int($self->Configuration->GetValue('runas_gid'));
		my $renice = int($self->Configuration->GetValue('renice'));
		
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
			$self->abort("Bitflu refuses to run as root");
		}
		
		$self->info("$0 is running with pid $$ ; uid = ($>|$<) / gid = ($)|$()");
	}
	
	
	##########################################################################
	# This should get called after starting the mainloop
	# The subroutine does the same as a 'init' in a plugin
	sub PreloopInit {
		my($self) = @_;
		$self->Admin->RegisterCommand('die'      , $self, '_Command_Shutdown'     , 'Terminates bitflu');
		$self->Admin->RegisterCommand('crashdump', $self, '_Command_Crashdump'    , 'Crashes bitflu (used for debugging)');
		$self->Admin->RegisterCommand('version'  , $self, '_Command_Version'      , 'Displays bitflu version string');
		$self->Admin->RegisterCommand('date'     , $self, '_Command_Date'         , 'Displays current time and date');
		$self->Admin->RegisterCommand('sysinfo'  , $self, '_Command_Sysinfo'      , 'Returns various system related informations');
	}
	
	##########################################################################
	# 'Crashes' bitflu
	sub _Command_Crashdump {
		my($self) = @_;
		$self->info("Crashdumping: ".Carp::cluck());
		exit(1);
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
		return {CHAINSTOP=>1, MSG=>[ [1, "This is Bitflu ".VERSION] ]};
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

	
	sub info  { my($self,$msg) = @_;  return if $self->Configuration->GetValue('loglevel') < 5;  print localtime()." # $msg\n"; }
	sub debug { my($self, $msg) = @_; return if $self->Configuration->GetValue('loglevel') < 10; print localtime()." # **  DEBUG  ** $msg\n"; }
	sub warn  { my($self,$msg) = @_;  return if $self->Configuration->GetValue('loglevel') < 2;  print localtime()." # ** WARNING ** $msg\n"; }
	sub abort { my($self, $msg) = @_; $self->info("## ABORTED ## $msg"); exit(1); }
	sub panic {
		my($self,$msg) = @_;
		$self->info("--------- BITFLU SOMEHOW MANAGED TO CRASH ITSELF; PANIC MESSAGE: ---------");
		$self->info($msg);
		$self->info("--------- BACKTRACE FOLLOWS ---------");
		Carp::cluck();
		$self->info("################################## ..phew!");
		exit(1);
	}
	
	
1;


####################################################################################################################################################
####################################################################################################################################################
# Bitflu Queue manager
#
package Bitflu::QueueMgr;
use Digest::SHA1;
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
			my $this_storage = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage for sid $sid");
			my $owner = $this_storage->GetSetting('owner');
			$self->info("Telling $owner to take care of $sid");
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
		my $msg = undef;
		foreach my $cid (@args) {
			my $storage = $self->{super}->Storage->OpenStorage($cid);
			if($storage) {
				my $owner = $storage->GetSetting('owner');
				if(defined($owner) && defined($runners->{$owner})) {
					$runners->{$owner}->cancel_this($cid);
					$msg .= "'$cid' has been canceled on $owner ;";
				}
				else {
					$msg .= "'$cid' has no owner! unable to remove download! ;";
				}
			}
			else {
				$msg .= "Unable to cancel '$cid' : Key not in queue ;";
			}
		}
		return({CHAINSTOP=>1, MSG=>[[undef,$msg]]});
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
		my $shaname = ($args{ShaName} || unpack("H*", Digest::SHA1::sha1($name)));
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
		$self->RegisterCommand("plugins", $self, 'admincmd_plugins', 'Displays all loaded plugins');
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
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }

1;


###############################################################################################################
# Bitflu Network-IO Lib : Release 20070319_1
package Bitflu::Network;

use strict;
use IO::Socket;
use IO::Select;
use POSIX;

use constant NETSTATS     => 2;
use constant MAXONWIRE    => 1024*1024; # Do not buffer more than 1mb per client connection (Fixme: We should reject enorminous requests as sent by azureus)
use constant BPS_MIN      => 8;

	##########################################################################
	# Creates a new Networking Object
	sub new {
		my($class, %args) = @_;
		my $self = {super=> $args{super}, bpc=>BPS_MIN, NOWTIME => undef , _bitflu_network => {}, avfds => 0,
		            stats => {nextrun=>0, sent=>0, recv=>0, raw_recv=>0, raw_sent=>0} };
		bless($self,$class);
		$self->SetTime;
		$self->{avfds} = $self->TestFileDescriptors;
		$self->info("Reserved $self->{avfds} file descriptors for networking");
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
	
	
	sub _Command_Netstat {
		my($self) = @_;
		my @A = ();
		my $bfn  = $self->{_bitflu_network};
		
		push(@A, [3, "Total file descriptors left : $self->{avfds}"]);
		
		foreach my $item (keys(%$bfn)) {
			if(defined($bfn->{$item}->{config})) {
				push(@A, [1, "Handle: $item"]);
				push(@A, [undef,"Active TCP connections:           $bfn->{$item}->{config}->{cntMaxPeers}"]);
				push(@A, [undef,"TCP connection hardlimit:         $bfn->{$item}->{config}->{MaxPeers}"]);
				push(@A, [undef,"Connections not yet established: ".int(keys(%{$bfn->{$item}->{establishing}}))]);
				push(@A, [undef, '']);
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
		while($i++ < 2048) {
			unless( open($fdx[$i], '/dev/zero') ) {
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
		$self->{NOWTIME} = int(time());
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
		return $self->{_bitflu_network}->{$socket}->{lastio};
	}
	
	##########################################################################
	# Try to create a new listening socket
	# NewTcpListen(ID=>UniqueueRunnerId, Port=>PortToListen, Bind=>IPv4ToBind)
	sub NewTcpListen {
		my($self,%args) = @_;
		return undef if(!defined($args{ID}));
		
		
		if(defined($self->{_bitflu_network}->{$args{ID}}->{socket})) {
			$self->panic("FATAL: $args{ID} has a listening socket, unable to create a second instance with the same ID");
		}
		
		$self->{_bitflu_network}->{$args{ID}} = { select => undef, socket => undef,  rqi => 0, wqi => 0,
		                                          config => { MaxPeers=>($args{MaxPeers}), cntMaxPeers=>0, Throttle=>($args{Throttle}||0) } };
		
		
		if(!defined($self->{_bitflu_network}->{$args{ID}}->{select})) {
			$self->{_bitflu_network}->{$args{ID}}->{select} = new IO::Select or $self->panic("Unable to create new IO::Select object: $!");
		}
		
		# Note:
		# select, socket, establishing and config are SELECT bound
		# outbuff is SOCKET bound
		
		my $new_socket = 0;
		if($args{Port}) {
			$new_socket = IO::Socket::INET->new(LocalPort=>$args{Port}, LocalAddr=>$args{Bind}, Proto=>'tcp', ReuseAddr=>1, Listen=>1) or return undef;
			$self->{_bitflu_network}->{$args{ID}}->{select}->add($new_socket) or $self->panic("Unable to glue <$new_socket> to select object of $args{ID}: $!");
		}
		$self->{_bitflu_network}->{$args{ID}}->{socket} = $new_socket;
		
		
		if($args{MaxPeers} < 1) {
			$self->panic("$args{ID} cannot reserve '$args{MaxPeers}' file descriptors");
		}
		
		
		
		return $new_socket;
	}
	
	
	##########################################################################
	# Creates a new (outgoing) connection
	# NewTcpConnection(ID=>UniqueRunnerId, Ipv4=>Ipv4, Port=>PortToConnect);
	sub NewTcpConnection {
		my($self, %args) = @_;
		return undef if(!defined($args{ID}));
		
		if(--$self->{avfds} < 0) {
			$self->{avfds}++;
			return undef;
		}
		
		if(++$self->{_bitflu_network}->{$args{ID}}->{config}->{cntMaxPeers} > $self->{_bitflu_network}->{$args{ID}}->{config}->{MaxPeers}) {
			$self->{_bitflu_network}->{$args{ID}}->{config}->{cntMaxPeers}--;
			$self->{avfds}++;
			return undef;
		}
		
		if(!defined($self->{_bitflu_network}->{$args{ID}}->{select})) {
			$self->{_bitflu_network}->{$args{ID}}->{select} = new IO::Select or $self->panic("Unable to create new IO::Select object: $!");
		}
		
		
		my $proto = getprotobyname('tcp');
		my $sock  = undef;
		socket($sock, AF_INET,SOCK_STREAM,$proto) or $self->panic("Failed to create a new socket : $!");
		
		my $sin = undef;
		eval { $sin = sockaddr_in($args{Port}, inet_aton($args{Ipv4})); };
		
		if(!defined($sin)) {
			$self->warn("Unable to create socket for $args{Ipv4}:$args{Port}");
			$self->{_bitflu_network}->{$args{ID}}->{config}->{cntMaxPeers}--;
			$self->{avfds}++;
			return undef;
		}
		
		
		$self->Unblock($sock) or $self->panic("Failed to unblock new socket <$sock> : $!");
		
		if(defined($self->{_bitflu_network}->{$args{ID}}->{establishing}->{$sock})) {
			$self->panic("FATAL: DUPLICATE SOCKET-ID?!");
		}
		
		# Write PerSocket information: establishing | outbuff | config
		$self->{_bitflu_network}->{$args{ID}}->{establishing}->{$sock} = {socket=>$sock, till=>$self->GetTime+$args{Timeout}, sin=>$sin};
		$self->{_bitflu_network}->{$sock}->{sockmap}   = $sock;
		$self->{_bitflu_network}->{$sock}->{handlemap} = $args{ID};
		
		return $sock;
	}
	
	
	##########################################################################
	# Run Network IO
	# Run(UniqueIdToRun,{callbacks});
	sub Run {
		my($self, $handle_id, $callbacks) = @_;
		
		my $select_handle = $self->{_bitflu_network}->{$handle_id}->{select} or return undef;
		
		$self->SetTime;
		$self->_Throttle;
		# Fixme: Sieht zwar lustig aus, sollte ich aber nach dem profiling wieder inlinen da es unnoetige calls generiert
		# und Run() wirklich ein hot-spot ist.
		$self->_Establish($handle_id, $callbacks, $select_handle);
		$self->_IOread($handle_id, $callbacks, $select_handle);
		
	#	print "Currently we could upload with $self->{bpc}\n";
		$self->_IOwrite($handle_id,$callbacks, $select_handle);
	}
	
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
				delete($self->{_bitflu_network}->{$handle_id}->{establishing}->{$ref->{socket}});
				delete($self->{_bitflu_network}->{$ref->{socket}});
				delete($self->{_bitflu_network}->{$handle_id}->{writeq}->{$ref->{socket}});
				close($ref->{socket});
			}
		}
	}
	
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
			my $tor = --$self->{_bitflu_network}->{$handle_id}->{rqi};
			my $socket = ${$self->{_bitflu_network}->{$handle_id}->{rq}}[$tor];
			if(defined($self->{_bitflu_network}->{$handle_id}->{socket}) && ($socket eq $self->{_bitflu_network}->{$handle_id}->{socket})) {
				my $new_sock = $socket->accept();
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
				elsif(++$self->{_bitflu_network}->{$handle_id}->{config}->{cntMaxPeers} > $self->{_bitflu_network}->{$handle_id}->{config}->{MaxPeers}) {
					$self->warn("Handle <$handle_id> is full: Dropping new socket");
					$self->{_bitflu_network}->{$handle_id}->{config}->{cntMaxPeers}--;
					$self->{avfds}++;
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
				}
				else {
					$self->{_bitflu_network}->{$new_sock}->{sockmap}   = $new_sock;
					$self->{_bitflu_network}->{$new_sock}->{handlemap} = $handle_id;
					$self->{_bitflu_network}->{$new_sock}->{lastio}    = $self->GetTime;
					$select_handle->add($new_sock);
					if(my $cbn = $callbacks->{Accept}) { $handle_id->$cbn($new_sock); }
				}
			}
			elsif(defined($self->{_bitflu_network}->{$socket})) {
				my $buffer = undef;
				my $bufflen = read($socket,$buffer,POSIX::BUFSIZ);
				if(defined($bufflen) && $bufflen != 0) {
					$self->{stats}->{raw_recv}                   += $bufflen;
					$self->{_bitflu_network}->{$socket}->{lastio} = $self->GetTime;
					if(my $cbn = $callbacks->{Data}) { $handle_id->$cbn($socket, \$buffer, $bufflen); }
				}
				else {
					if(my $cbn = $callbacks->{Close}) { $handle_id->$cbn($socket); }
					$self->RemoveSocket($handle_id,$socket);
				}
			}
			last if --$rpr < 0;
		}
		
		
	}
	
	sub _IOwrite {
		my($self, $handle_id, $callbacks, $select_handle) = @_;
		my $bufsiz            = $self->{bpc};
		   $bufsiz            = POSIX::BUFSIZ if $self->{_bitflu_network}->{$handle_id}->{config}->{Throttle} == 0;
		
		if($self->{_bitflu_network}->{$handle_id}->{wqi} == 0) {
			# Refill cache
			my @sq = (keys(%{$self->{_bitflu_network}->{$handle_id}->{writeq}}));
			$self->{_bitflu_network}->{$handle_id}->{wq} = \@sq;
			$self->{_bitflu_network}->{$handle_id}->{wqi} = int(@sq);
		}
		
		my $wpr = $self->{super}->Configuration->GetValue('writepriority');
		
		while($self->{_bitflu_network}->{$handle_id}->{wqi} > 0) {
			my $tow = --$self->{_bitflu_network}->{$handle_id}->{wqi};
			my $ssocket = ${$self->{_bitflu_network}->{$handle_id}->{wq}}[$tow];
			next unless defined($self->{_bitflu_network}->{$ssocket});
			
			my $wsocket    = $self->{_bitflu_network}->{$ssocket}->{sockmap};
			
			if(!defined($wsocket))                { $self->panic("Sockmap corrupted, you shouldn't see this message."); }
			if(!$select_handle->exists($wsocket)) { next; } # not yet connected
			
			my $bytes_sent = syswrite($wsocket, $self->{_bitflu_network}->{$wsocket}->{outbuff},$bufsiz);
			
			if($!{'EISCONN'}) {
				$self->debug("EISCONN returned.");
			}
			elsif(!defined($bytes_sent)) {
				if(my $cbn = $callbacks->{Close}) { $handle_id->$cbn($wsocket); }
				delete($self->{_bitflu_network}->{$handle_id}->{writeq}->{$wsocket}) or $self->panic;
				$self->RemoveSocket($handle_id,$wsocket);
			}
			else {
				$self->{stats}->{raw_sent} += $bytes_sent;
				$self->{_bitflu_network}->{$wsocket}->{qlen} -= $bytes_sent;
				$self->{_bitflu_network}->{$wsocket}->{outbuff} = substr($self->{_bitflu_network}->{$wsocket}->{outbuff},$bytes_sent);
				if($self->{_bitflu_network}->{$wsocket}->{qlen} == 0) {
					delete($self->{_bitflu_network}->{$handle_id}->{writeq}->{$wsocket});
				}
			}
			last if --$wpr < 0;
		}
	}
	


	
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
	# Write Data to socket
	# WriteData(UniqueRunId, Socket, DataToWRite)
	sub WriteData {
		my($self, $socket, $buffer) = @_;
		
		
		my $gotspace     = 1;
		my $queued_bytes = ($self->{_bitflu_network}->{$socket}->{qlen} or 0);
		my $this_bytes   = length($buffer);
		my $total_bytes  = $queued_bytes + $this_bytes;
		
		
		if($total_bytes > MAXONWIRE) {
			$self->warn("<$socket> Buffer overflow! Too much unsent data: $total_bytes bytes");
			$gotspace = 0;
		}
		else {
			my $handle_id = $self->{_bitflu_network}->{$socket}->{handlemap} or $self->panic("No handleid?");
			$self->{_bitflu_network}->{$socket}->{outbuff}              .= $buffer;
			$self->{_bitflu_network}->{$socket}->{lastio}                = $self->GetTime;
			$self->{_bitflu_network}->{$socket}->{qlen}                  = $total_bytes;
			$self->{_bitflu_network}->{$handle_id}->{writeq}->{$socket}  = $socket; # Triggers a new write
		}
		return $gotspace;
	}
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
	
	
	sub Unblock {
		my($self, $cfh) = @_;
		$self->panic("No filehandle given") unless $cfh;
		my $flags = fcntl($cfh, F_GETFL, 0)
								or return undef;
		fcntl($cfh, F_SETFL, $flags | O_NONBLOCK)
		or return undef;
		return 1;
	}

	
1;


###############################################################################################################
# Builtin Configuration parser / class
package Bitflu::Configuration;
use strict;


	sub new {
		my($class, %args) = @_;
		my $self = { configuration_file => $args{configuration_file}, super=> $args{super} };
		bless($self, $class);
		if (-f $self->{configuration_file}) {
			open(CFGH, "+<", $self->{configuration_file}) or die("Unable to open $self->{configuration_file} for writing: $!\n");
			$self->{configuration_fh} = *CFGH;
		}
		else {
			warn("Reading configuration file '".$self->{configuration_file}."' failed: $!\n");
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
		$self->{conf}->{sleeper}         = 0.025;
		$self->{conf}->{writepriority}   = 2;
		$self->{conf}->{readpriority}    = 4;
		$self->{conf}->{loglevel}        = 5;
		$self->{conf}->{renice}          = 8;
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



