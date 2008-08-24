#!/usr/bin/perl -w
#
# This file is part of 'Bitflu' - (C) 2006-2008 Adrian Ulrich
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

GetOptions($getopts, "help|h", "version", "plugins", "config=s", "daemon") or exit 1;
if($getopts->{help}) { die "Usage: $0 [--config=.bitflu.config --version --help --plugins --daemon]\n"; }


# -> Create bitflu object
my $bitflu = Bitflu->new(configuration_file=>$getopts->{config}) or Carp::confess("Unable to create Bitflu Object");
if($getopts->{version}) { die $bitflu->_Command_Version->{MSG}->[0]->[1]."\n" }

my @loaded_plugins = $bitflu->LoadPlugins('Bitflu');
if($getopts->{plugins}) {
	print "# Loaded Plugins: (from ".$bitflu->Configuration->GetValue('plugindir').")\n";
	foreach (@loaded_plugins) { printf("File %-35s provides: %s\n", $_->{file}, $_->{package}); }
	exit(0);
}
elsif($getopts->{daemon}) {
	$bitflu->Daemonize();
}

$bitflu->SysinitProcess();
$bitflu->SetupDirectories();
$bitflu->InitPlugins();
$bitflu->PreloopInit();

$bitflu_run = 1 if !defined($bitflu_run); # Enable mainloop and sighandler if we are still not_killed



while($bitflu_run == 1) {
	foreach my $x (@{$bitflu->{_Runners}}) {
		$x->run();
	}
	select(undef,undef,undef,$bitflu->Configuration->GetValue('sleeper'));
}

$bitflu->Storage->terminate;

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
use constant VERSION => "0.52-stable";
use constant APIVER  => 20080611;
use constant LOGBUFF => 0xFF;

	##########################################################################
	# Create a new Bitflu-'Dispatcher' object
	sub new {
		my($class, %args) = @_;
		my $self = {};
		bless($self, $class);
		$self->{_LogFH}                 = *STDOUT;                            # Must be set ASAP
		$self->{_LogBuff}               = [];                                 # Empty at startup
		$self->{Core}->{Tools}          = Bitflu::Tools->new(super => $self); # Tools is also loaded ASAP because ::Configuration needs it
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
	# Call hardcoded tools plugin
	sub Tools {
		my($self) = @_;
		return $self->{Core}->{Tools};
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
		my %exclude  = (map { $_ => 1} split(/;/,$self->Configuration->GetValue('pluginexclude')));
		
		opendir(PLUGINS, $pdirpath) or $self->stop("Unable to read directory '$pdirpath' : $!");
		foreach my $dirent (sort readdir(PLUGINS)) {
			next unless my($pfile, $porder, $pmodname) = $dirent =~ /^((\d\d)_(.+)\.pm)$/i;
			
			if($exclude{$pfile}) {
				$self->info("Skipping disabled plugin '$pfile -> $pmodname'");
			}
			elsif($porder eq '00' && $pmodname ne $self->Configuration->GetValue('storage')) {
				$self->debug("Skipping unconfigured storage plugin '$dirent'");
			}
			else {
				push(@plugins, {file=>$pfile, order=>$porder, class=>$xclass, modname=>$pmodname, package=>$xclass."::".$3});
				$self->debug("Found plugin $plugins[-1]->{package} in folder $pdirpath");
			}
		}
		close(PLUGINS);
		
		$self->{_Plugins} = \@plugins;
		
		foreach my $plugin (@{$self->{_Plugins}}) {
			my $fname = $plugin->{class}."/".$plugin->{file};
			$self->debug("Loading $fname");
			eval { require $fname; };
			if($@) {
				my $perr = $@; chomp($perr);
				$self->yell("Unable to load plugin '$fname', error was: '$perr'");
				$self->stop(" -> Please fix or remove this broken plugin file from $pdirpath");
			}
			my $this_apiversion = $plugin->{package}->_BITFLU_APIVERSION;
			if($this_apiversion != APIVER) {
				$self->yell("Plugin '$fname' has an invalid API-Version ( (\$apivers = $this_apiversion) != (\$expected = ".APIVER.") )");
				$self->yell("HINT: Maybe you forgot to replace the plugins at $pdirpath while upgrading bitflu?!...");
				$self->stop("-> Exiting due to APIVER mismatch");
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
	# Build some basic directory structure
	sub SetupDirectories {
		my($self) =@_;
		my $workdir = $self->Configuration->GetValue('workdir') or $self->panic("No workdir configured");
		my $tmpdir  = $self->Configuration->GetValue('tempdir') or $self->panic("No tempdir configured");
		$tmpdir = $workdir."/".$tmpdir;
		foreach my $this_dir ($workdir, $tmpdir) {
			unless(-d $this_dir) {
				$self->debug("mkdir($this_dir)");
				mkdir($this_dir) or $self->stop("Unable to create directory '$this_dir' : $!");
			}
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
		my $outlog = ($self->Configuration->GetValue('logfile')      || '');
		
		
		if(length($outlog) > 0) {
			open(LFH, ">>", $outlog) or $self->stop("Cannot write to logfile '$outlog' : $!");
			$self->{_LogFH} = *LFH;
			$self->{_LogFH}->autoflush(1);
			$self->yell("Logging to '$outlog'");
		}
		
		
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
	}
	
	sub Daemonize {
		my($self) = @_;
		$self->info("Backgrounding");
		my $child = fork();
		
		if(!defined($child)) {
			die "Unable to fork: $!\n";
		}
		elsif($child != 0) {
			$self->yell("Bitflu is running with pid $child");
			exit(0);
		}
	}
	
	##########################################################################
	# bye!
	sub _Command_Shutdown {
		my($self) = @_;
		kill(2,$$);
		return {MSG=>[ [1, "Shutting down $0 (with pid $$)"] ], SCRAP=>[]};
	}

	##########################################################################
	# Return version string
	sub _Command_Version {
		my($self) = @_;
		my $uptime = ( ($self->Network->GetTime - $self->{_BootTime}) / 60);
		return {MSG=>[ [1, sprintf("This is Bitflu %s (%s) running on Perl %vd. Uptime: %.3f minutes (%s)",VERSION, APIVER, $^V, $uptime,
		                                         "".localtime($self->{_BootTime}) )] ], SCRAP=>[]};
	}

	##########################################################################
	# Return version string
	sub _Command_Date {
		my($self) = @_;
		return {MSG=>[ [1, "".localtime()] ], SCRAP=>[]};
	}
	
	##########################################################################
	# Printout logmessage
	sub _xlog {
		my($self, $msg, $force_stdout) = @_;
		my $rmsg  = localtime()." # $msg\n";
		my $xfh   = $self->{_LogFH};
		my $lbuff = $self->{_LogBuff};
		
		print $xfh $rmsg;
		
		if($force_stdout && $xfh ne *STDOUT) {
			print STDOUT $rmsg;
		}
		
		push(@$lbuff, $rmsg);
		shift(@$lbuff) if int(@$lbuff) >= LOGBUFF;
	}
	
	sub info  { my($self,$msg) = @_; return if $self->Configuration->GetValue('loglevel') < 4;  $self->_xlog($msg);                 }
	sub debug { my($self,$msg) = @_; return if $self->Configuration->GetValue('loglevel') < 10; $self->_xlog(" ** DEBUG **  $msg"); }
	sub warn  { my($self,$msg) = @_; return if $self->Configuration->GetValue('loglevel') < 2;  $self->_xlog("** WARNING ** $msg"); }
	sub yell  { my($self,$msg) = @_; $self->_xlog($msg,1);                                                                          }
	sub stop  { my($self,$msg) = @_; $self->yell("EXITING # $msg"); exit(1); }
	sub panic {
		my($self,$msg) = @_;
		$self->yell("--------- BITFLU SOMEHOW MANAGED TO CRASH ITSELF; PANIC MESSAGE: ---------");
		$self->yell($msg);
		$self->yell("--------- BACKTRACE START ---------");
		$self->yell(Carp::longmess());
		$self->yell("---------- BACKTRACE END ----------");
		
		$self->yell("SHA1-Module used : ".$self->Tools->{mname});
		$self->yell("Perl Version     : ".sprintf("%vd", $^V));
		$self->yell("Perl Execname    : ".$^X);
		$self->yell("OS-Name          : ".$^O);
		$self->yell("Running since    : ".gmtime($self->{_BootTime}));
		$self->yell("---------- LOADED PLUGINS ---------");
		foreach my $plug (@{$self->{_Plugins}}) {
			$self->yell(sprintf("%-32s -> %s",$plug->{file}, $plug->{package}));
		}
		$self->yell("##################################");
		exit(1);
	}
	
	
1;


####################################################################################################################################################
####################################################################################################################################################
# Bitflu Queue manager
#
package Bitflu::QueueMgr;

use constant SHALEN => 40;
use constant HPFX   => 'history_';

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
		
		$self->{super}->Admin->RegisterCommand('history' , $self, 'admincmd_history', 'Manages download history',
		        [  [undef, "Manages internal download history"], [undef, ''],
		           [undef, "Usage: history [ queue_id [show forget] ] [list]"], [undef, ''],
		           [undef, "history list            : List all remembered downloads"],
		           [undef, "history queue_id show   : Shows details about queue_id"],
		           [undef, "history queue_id forget : Removes history of queue_id"],
		        ]);
		
		$self->{super}->Admin->RegisterCommand('pause' , $self, 'admincmd_pause', 'Stops a download',
		        [  [undef, "Stop given download"], [undef, ''],
		           [undef, "Usage: pause queue_id [queue_id2 ...]"], [undef, ''],
		        ]);
		
		$self->{super}->Admin->RegisterCommand('resume' , $self, 'admincmd_resume', 'Resumes a paused download',
		        [  [undef, "Resumes a paused download"], [undef, ''],
		           [undef, "Usage: resume queue_id [queue_id2 ...]"], [undef, ''],
		        ]);
		
		$self->info("--- startup completed: bitflu is ready ---");
		return 1;
	}
	
	
	##########################################################################
	# Pauses a download
	sub admincmd_pause {
		my($self, @args) = @_;
		
		my @MSG    = ();
		my $NOEXEC = '';
		
		if(int(@args)) {
			foreach my $cid (@args) {
				my $so = $self->{super}->Storage->OpenStorage($cid);
				if($so) {
					$so->SetSetting('_paused', 1);
					push(@MSG, [1, "$cid: download paused"]);
				}
				else {
					push(@MSG, [2, "$cid: does not exist in queue, cannot pause"]);
				}
			}
		}
		else {
			$NOEXEC .= 'Usage: pause queue_id';
		}
		return({MSG=>\@MSG, SCRAP=>[], NOEXEC=>$NOEXEC});
	}
	
	##########################################################################
	# Resumes a download
	sub admincmd_resume {
		my($self, @args) = @_;
		
		my @MSG    = ();
		my $NOEXEC = '';
		
		if(int(@args)) {
			foreach my $cid (@args) {
				my $so = $self->{super}->Storage->OpenStorage($cid);
				if($so) {
					$so->SetSetting('_paused', 0);
					push(@MSG, [1, "$cid: download resumed"]);
				}
				else {
					push(@MSG, [2, "$cid: does not exist in queue, cannot resume"]);
				}
			}
		}
		else {
			$NOEXEC .= 'Usage: resume queue_id';
		}
		return({MSG=>\@MSG, SCRAP=>[], NOEXEC=>$NOEXEC});
	}
	
	
	##########################################################################
	# Cancel a queue item
	sub admincmd_cancel {
		my($self, @args) = @_;
		
		my $runners = $self->GetRunnersRef();
		my @MSG     = ();
		my $NOEXEC  = '';
		
		if(int(@args)) {
			foreach my $cid (@args) {
				my $storage = $self->{super}->Storage->OpenStorage($cid);
				if($storage) {
					my $owner = $storage->GetSetting('owner');
					if(defined($owner) && defined($runners->{$owner})) {
						$self->ModifyHistory($cid, Canceled=>'');
						$runners->{$owner}->cancel_this($cid);
						push(@MSG, [1, "'$cid' canceled"]);
					}
					else {
					$self->panic("'$cid' has no owner, cannot cancel!");
					}
				}
				else {
					push(@MSG, [2, "'$cid' not removed from queue: No such item"]);
				}
			}
		}
		else {
			$NOEXEC .= 'Usage: cancel queue_id [queue_id2 ...]';
		}
		
		
		return({MSG=>\@MSG, SCRAP=>[], NOEXEC=>$NOEXEC});
	}
	
	##########################################################################
	# Rename a queue item
	sub admincmd_rename {
		my($self, @args) = @_;
		
		my $sha    = $args[0];
		my $name   = $args[1];
		my @MSG    = ();
		my $NOEXEC = '';
		
		if(!defined($name)) {
			$NOEXEC .= "Usage: rename queue_id \"New Name\"";
		}
		elsif(my $storage = $self->{super}->Storage->OpenStorage($sha)) {
			$storage->SetSetting('name', $name);
			push(@MSG, [1, "Renamed $sha into '$name'"]);
		}
		else {
			push(@MSG, [2, "Unable to rename $sha: queue_id does not exist"]);
		}
		return({MSG=>\@MSG, SCRAP=>[], NOEXEC=>$NOEXEC});
	}
	
	##########################################################################
	# Manages download history
	sub admincmd_history {
		my($self,@args) = @_;
		
		my $sha = ($args[0] || '');
		my $cmd = ($args[1] || '');
		my @MSG    = ();
		my $NOEXEC = '';
		my $hpfx   = HPFX;
		my $hkey   = $hpfx.$sha;
		my $strg   = $self->{super}->Storage;
		
		if($sha eq 'list') {
			my @cbl = $strg->ClipboardList;
			my $cbi = 0;
			foreach my $item (@cbl) {
				if(my($this_sid) = $item =~ /^$hpfx(.+)$/) {
					my $ll = "$1 : ".substr($self->GetHistory($this_sid)->{Name},0,64);
					push(@MSG, [ ($strg->OpenStorage($this_sid) ? 1 : 5 ), $ll]);
					$cbi++;
				}
			}
			push(@MSG, [1, "$cbi item".($cbi == 1 ? '' : 's')." stored in history"]);
		}
		elsif(length($sha)) {
			if(my $ref = $self->GetHistory($sha)) {
				if($cmd eq 'show') {
					foreach my $k (sort keys(%$ref)) {
						push(@MSG,[1, sprintf("%20s -> %s",$k,$ref->{$k})]);
					}
				}
				elsif($cmd eq 'forget') {
					$strg->ClipboardRemove($hkey);
					push(@MSG, [1, "history for $sha has been removed"]);
				}
				else {
					push(@MSG, [2, "unknown subcommand, see 'help history'"]);
				}
			}
			else {
				push(@MSG, [2,"queue item $sha has no history"]);
			}
		}
		else {
			push(@MSG, [2,"See 'help history'"]);
		}
		
		return({MSG=>\@MSG, SCRAP=>[], NOEXEC=>$NOEXEC});
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
		my $shaname = ($args{ShaName} || unpack("H*", $self->{super}->Tools->sha1($name)));
		my $owner   = ref($args{Owner}) or $self->panic("No owner?");
		my $sobj    = 0;
		my $history = $self->{super}->Configuration->GetValue('history');
		
		if($size == 0 && $chunks != 1) {
			$self->panic("Sorry: You can not create a dynamic storage with multiple chunks ($chunks != 1)");
		}
		if(!defined($name)) {
			$self->panic("AddItem needs a name!");
		}
		if(length($shaname) != SHALEN) {
			$self->panic("Invalid shaname: $shaname");
		}
		
		
		if($self->{super}->Storage->OpenStorage($shaname)) {
			$@ = "$shaname: item exists in queue";
		}
		elsif($history && $self->GetHistory($shaname)) {
			$@ = "$shaname: has already been downloaded. Use 'history $shaname forget' if you want do re-download it";
			$self->warn($@);
		}
		elsif($sobj = $self->{super}->Storage->CreateStorage(StorageId => $shaname, Size=>$size, Chunks=>$chunks, Overshoot=>$overst, FileLayout=>$flayout)) {
			$sobj->SetSetting('owner', $owner);
			$sobj->SetSetting('name' , $name);
			$sobj->SetSetting('createdat', $self->{super}->Network->GetTime);
			if($history) {
				$self->ModifyHistory($shaname, Name=>$name, Canceled=>'never', Started=>'',
				                               Ended=>'never', Committed=>'never');
			}
		}
		else {
			$self->panic("CreateStorage for $shaname failed");
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
	# Updates/creates on-disk history of given sid
	# Note: Strings with length == 0 are replaced with the current time. Awkward.
	sub ModifyHistory {
		my($self,$sid, %args) = @_;
		if($self->{super}->Storage->OpenStorage($sid)) {
			my $old_ref = $self->GetHistory($sid);
			foreach my $k (keys(%args)) {
				my $v = $args{$k};
				$v = "".localtime($self->{super}->Network->GetTime) if length($v) == 0;
				$old_ref->{$k} = $v;
			}
			return $self->{super}->Storage->ClipboardSet(HPFX.$sid, $self->{super}->Tools->RefToCBx($old_ref));
		}
		else {
			return 0;
		}
	}
	
	##########################################################################
	# Returns history of given sid
	sub GetHistory {
		my($self,$sid) = @_;
		my $r = $self->{super}->Tools->CBxToRef($self->{super}->Storage->ClipboardGet(HPFX.$sid));
		return $r;
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
	# Returns true if download is marked as paused
	sub IsPaused {
		my($self,$sid) = @_;
		my $so = $self->{super}->Storage->OpenStorage($sid) or $self->panic("$sid does not exist");
		return ( $so->GetSetting('_paused') ? 1 : 0 );
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
	
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug("QueueMGR: ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("QueueMGR: ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("QueueMGR: ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("QueueMGR: ".$msg); }

1;

###############################################################################################################
# Bitflu Sammelsurium
package Bitflu::Tools;

	use MIME::Base64 ();
	use IO::Socket;
	
	##########################################################################
	# Create new object and try to load a module
	# Note: new() this gets called before ::Configuration is ready!
	#       You can't use fancy stuff such as ->debug. ->stop should work
	sub new {
		my($class, %args) = @_;
		my $self = { super => $args{super}, ns => '', mname => '' };
		bless($self,$class);
		
		foreach my $mname (qw(Digest::SHA Digest::SHA1)) {
			my $code = "use $mname; \$self->{ns} = $mname->new; \$self->{mname} = \$mname";
			eval $code;
		}
		
		unless($self->{mname}) {
			$self->stop("No SHA1-Module found. Bitflu requires 'Digest::SHA' (http://search.cpan.org)");
		}
		
		return $self;
	}
	
	sub init { return 1 }
	
	##########################################################################
	# Return hexed sha1 of $buff
	sub sha1_hex {
		my($self, $buff) = @_;
		$self->{ns}->add($buff);
		return $self->{ns}->hexdigest;
	}
	
	##########################################################################
	# Return sha1 of $buff
	sub sha1 {
		my($self,$buff) = @_;
		$self->{ns}->add($buff);
		return $self->{ns}->digest;
	}
	
	##########################################################################
	# Encode string into base32
	sub encode_b32 {
		my($self,$val) = @_;
		my $s = unpack("B*",$val);
		$s =~ s/(.{5})/000$1/g; # Convert 5-byte-chunks to 8-byte-chunks
		my $len  = length($s);
		my $olen = $len % 8;
		
		if($olen) {
			$s = substr($s,0,$len-$olen)."000".substr($s,-1*$olen).("0" x (5 - $olen));
		}
		
		$s = pack("B*",$s);
		$s =~ tr/\0-\37/A-Z2-7/; # Octal!
		return $s;
	}
	
	##########################################################################
	# Decode base32 into string
	sub decode_b32 {
		my($self,$val) = @_;
		my $s = uc($val);
		$s =~ tr/A-Z2-7/\0-\37/;
		$s = unpack("B*", $s);
		$s =~ s/000(.{5})/$1/g;
		if( my $olen = -1*(length($s)%8) ) {
			$s = substr($s,0,$olen);
		}
		return pack("B*",$s);
	}
	
	##########################################################################
	# Decode base64 into string
	sub decode_b64 {
		my($self,$val) = @_;
		return MIME::Base64::decode($val);
	}
	
	
	##########################################################################
	# Parse a magnet link
	sub decode_magnet {
		my($self,$uri) = @_;
		my $xt = {};
		if($uri =~ /^magnet:\?(.+)$/) {
			foreach my $item (split(/&/,$1)) {
				if($item =~ /^(([^=\.]+)(\.\d+)?)=(.+)$/) {
					my $mk = $2;
					my @it  = split(/:/,$4);
					my $mv = pop(@it);
					my $sk = join(':',@it) || ":";
					push(@{$xt->{$mk}}, {$sk => $mv});
				}
			}
		}
		return $xt;
	}
	
	sub ExpandRange {
		my($self,@a) = @_;
		my %dedupe = ();
		foreach my $chunk (@a) {
			if($chunk =~ /^(\d+)-(\d+)$/) { if($2 <= 0xFFFF) { for($1..$2) { $dedupe{$_} = 1; } } }
			elsif($chunk =~ /^(\d+)$/)    { $dedupe{abs($1)} = 1;                                 }
		}
		return \%dedupe;
	}
	
	##########################################################################
	# Resolve hostnames
	sub Resolve {
		my($self,$name) = @_;
		my @iplist = ();
		my @result = gethostbyname($name);
		@iplist = map{ inet_ntoa($_) } @result[4..$#result];
		return List::Util::shuffle(@iplist);
	}
	
	##########################################################################
	# Escape a HTTP-URI-Escaped string
	sub UriUnescape {
		my($self,$string) = @_;
		$string =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;
		return $string;
	}
	
	##########################################################################
	# Escape string
	sub UriEscape {
		my($self,$string) = @_;
		$string =~ s/([^A-Za-z0-9\-_.!~*'()\/])/sprintf("%%%02X",ord($1))/eg;
		return $string;
	}
	
	##########################################################################
	# Converts a CBX into a hashref
	sub CBxToRef {
		my($self,$buff) = @_;
		my $r      = undef;
		   $buff ||= '';
		foreach my $line (split(/\n/,$buff)) {
			chomp($line);
			if($line =~ /^#/ or $line =~ /^\s*$/) {
				next; # Comment or empty line
			}
			elsif($line =~ /^([a-zA-Z0-9_\.]+)\s*=\s*(.*)$/) {
				$r->{$1} = $2;
			}
			else {
				# Ignore. Can't use panic anyway
			}
		}
		return $r;
	}
	
	##########################################################################
	# Convert hashref into CBX
	sub RefToCBx {
		my($self,$ref) = @_;
		my @caller = caller;
		my $buff   = "# Written by $caller[0]\@$caller[2] on ".gmtime()."\n";
		foreach my $key (sort(keys(%$ref))) {
			my $val = $ref->{$key};
			$key =~ tr/a-zA-Z0-9_\.//cd;
			$val =~ tr/\r\n//d;
			$buff .= sprintf("%-25s = %s\n",$key, $val);
		}
		return $buff."# EOF #\n";
	}
	
	##########################################################################
	# Generates a 'find' like dirlist
	sub GenDirList {
		my($self,$dstruct, $dir) = @_;
		push(@{$dstruct->{_}},$dir);
		my $pfx = join('/',@{$dstruct->{_}});
		opendir(DIR, $pfx);
		foreach my $dirent (readdir(DIR)) {
			my $fp = "$pfx/".$dirent;
			next if $dirent eq '.';   # No thanks
			next if $dirent eq '..';  # Ditto
			next if (-l $fp);         # Won't look at symlinks
			push(@{$dstruct->{list}},$fp);
			$self->GenDirList($dstruct,$dirent) if -d $fp;
		}
		closedir(DIR);
		pop(@{$dstruct->{_}});
	}
	
	##########################################################################
	# Getopts like support
	sub GetOpts {
		my($self,$args) = @_;
		my @leftovers = ();
		my $ctx       = undef;
		my $argref    = {};
		foreach my $this_arg (@$args) {
			if($this_arg =~ /^--?(.+)/) {
				$ctx = $1;
				$argref->{$ctx} = defined if !exists $argref->{$ctx};
			}
			elsif(defined($ctx)) {
				$argref->{$ctx} = $this_arg;
				$ctx = undef;
			}
			else {
				push(@leftovers, $this_arg);
			}
		}
		@$args = @leftovers;
		return $argref;
	}
	
	##########################################################################
	# Return exclusive name
	sub GetExclusiveDirectory {
		my($self,$base,$id) = @_;
		my $xname = undef;
		foreach my $sfx (0..0xFFFF) {
			$xname = $base."/".$id;
			$xname .= ".$sfx" if $sfx != 0;
			unless(-e $xname) {
				return $xname;
			}
		}
		return undef;
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
		$self->RegisterCommand("log",  $self, 'admincmd_log', 'Display last log output',
		 [ [undef, "Usage: log [-limit]"], [undef, 'Example: log -10   # <-- displays the last 10 log entries'] ] );
		return 1;
	}
	
	##########################################################################
	# Return logbuffer
	sub admincmd_log {
		my($self, @args) = @_;
		my @A       = ();
		my @log     = @{$self->{super}->{_LogBuff}};
		my $opts    = $self->{super}->Tools->GetOpts(\@args);
		my $limit   = int(((keys(%$opts))[0]) || 0);
		my $logsize = int(@log);
		my $logat   = ( $limit ? ( $logsize - $limit ) : 0 );
		my $i       = 0;
		
		foreach my $ll (@log) {
			next if $i++ < $logat;
			chomp($ll);
			push(@A, [undef, $ll]);
		}
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	##########################################################################
	# Display registered plugins
	sub admincmd_plugins {
		my($self) = @_;
		
		my @A = ([1, "Hooks registered at bitflus NSFS (NotSoFairScheduler)"]);
		foreach my $r (@{$self->{super}->{_Runners}}) {
			push(@A,[undef,$r]);
		}
		
		return({MSG=>\@A, SCRAP=>[]});
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
		
		
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	##########################################################################
	# Handles useradm commands
	sub admincmd_useradm {
		my($self, @args) = @_;
		
		my @A   = ();
		my $ERR = '';
		
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
			$ERR .= "Usage error, type 'help useradmin' for more information";
		}
		return({MSG=>\@A, SCRAP=>[], NOEXEC=>$ERR});
	}
	
	##########################################################################
	# Create password entry
	sub __useradm_mkentry {
		my($self,$usr,$pass) = @_;
		$usr =~ tr/: ;=//d;
		return undef if length($usr) == 0;
		return $usr.":".$self->{super}->Tools->sha1_hex("$usr;$pass");
	}
	
	##########################################################################
	# Modify current setting
	sub __useradm_modify {
		my($self,%args) = @_;
		my @result    = ();
		my $allusr    = {};
		my $to_inject = '';
		my $delta     = 0;
		foreach my $entry (split(/;/,($self->{super}->Configuration->GetValue('useradm') || ''))) {
			if(my($user,$hash) = $entry =~ /^([^:]*):(.+)$/) {
				if ($user ne ($args{Inject}->{User} || '') && $user ne ($args{Drop}->{User} || '')) {
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
		
		
		my $plugin_hits  = 0;
		my $plugin_fails = 0;
		my $plugin_ok    = 0;
		my @plugin_msg   = ();
		my @plugin_nex   = ();
		
		if(ref($self->GetCommands->{$command}) eq "ARRAY") {
			foreach my $ref (@{$self->GetCommands->{$command}}) {
				$plugin_hits++;
				my $class = $ref->{class};
				my $cmd   = $ref->{cmd};
				my $bref  = $class->$cmd(@args);
				my $SCRAP = $bref->{SCRAP}  or $self->panic("$class -> $cmd returned no SCRAP");
				my $MSG   = $bref->{MSG}    or $self->panic("$class -> $cmd returned no MSG");
				my $ERR   = $bref->{NOEXEC};
				@args     = @$SCRAP;
				
				push(@plugin_msg, @$MSG);
				
				if($ERR) {
					push(@plugin_nex, $ERR); # Plugin usage error
				}
				else {
					$plugin_ok++; # Plugin could do something
				}
			}
		}
		
		if($plugin_hits == 0) {
			push(@plugin_msg, [2, "Unknown command '$command'"]);
			$plugin_fails++;
		}
		else {
			foreach my $leftover (@args) {
				push(@plugin_msg, [2, "Failed to execute '$command $leftover'"]);
				$plugin_fails++;
			}
			
			if($plugin_ok == 0) {
				# Nothing executed, display all usage 'hints'
				foreach my $xerr (@plugin_nex) {
					push(@plugin_msg, [2, $xerr]);
					$plugin_fails++;
				}
			}
		}
		return({MSG=>\@plugin_msg, FAILS=>$plugin_fails});
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
	
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("Admin   : ".$msg);  }
	sub debug { my($self, $msg) = @_; $self->{super}->debug("Admin   : ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("Admin   : ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("Admin   : ".$msg); }

1;


###############################################################################################################
# Bitflu Network-IO Lib : Release 20071220_1
package Bitflu::Network;

use strict;
use IO::Socket;
use IO::Select;
use POSIX;

use constant NETSTATS     => 2;             # ReGen netstats each 2 seconds
use constant MAXONWIRE    => 1024*1024;     # Do not buffer more than 1mb per client connection
use constant BPS_MIN      => 8;             # Minimal upload speed per socket
use constant DEVNULL      => '/dev/null';   # Path to /dev/null
use constant LT_UDP       => 1;             # Internal ID for UDP sockets
use constant LT_TCP       => 2;             # Internal ID for TCP sockets
use constant BLIST_LIMIT  => 255;           # NeverEver blacklist more than 255 IPs per instance

	##########################################################################
	# Creates a new Networking Object
	sub new {
		my($class, %args) = @_;
		my $self = {super=> $args{super}, bpc=>BPS_MIN, NOWTIME => 0, timeflux=>0 , _bitflu_network => {}, avfds => 0,
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
		$self->{super}->Admin->RegisterCommand('netstat'    , $self, '_Command_Netstat',   'Displays networking information');
		$self->{super}->Admin->RegisterCommand('blacklist'  , $self, '_Command_Blacklist', 'Show current in-memory IP-Blacklist');
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
			if(exists($bfn->{$item}->{config})) {
				push(@A, [4, '-------------------------------------------------------------------------']);
				push(@A, [1, "Handle: $item"]);
				push(@A, [undef,"Active connections              : $bfn->{$item}->{config}->{cntMaxPeers}"]);
				push(@A, [undef,"Connection hardlimit            : $bfn->{$item}->{config}->{MaxPeers}"]);
				push(@A, [undef,"Connections not yet established : ".int(keys(%{$bfn->{$item}->{establishing}}))]);
			}
		}
		
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	sub _Command_Blacklist {
		my($self) = @_;
		my @A = ();
		my $bfn = $self->{_bitflu_network};
		
		foreach my $item (sort keys(%$bfn)) {
			if(exists($bfn->{$item}->{config})) {
				push(@A, [4, "Blacklist for ID $item"]);
				my $blc = 0;
				foreach my $k (keys(%{$bfn->{$item}->{blacklist}->{bldb}})) {
					push(@A, [2, "     $k"]);
					$blc++;
				}
				push(@A, [3, "$blc ip(s) are blacklisted"], [undef, '']);
			}
		}
		
		return({MSG=>\@A, SCRAP=>[]});
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
		
		my $NOW = time();
		
		if($NOW > $self->{NOWTIME}) {
			$self->{NOWTIME} = $NOW;
		}		
		elsif($NOW < $self->{NOWTIME}) {
			$self->warn("Clock jumped backwards! Returning last known good time...");
		}
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
	# Returns QueueLength of given socket
	sub GetQueueLen {
		my($self, $socket) = @_;
		$self->panic("Cannot return qlen of vanished socket <$socket>") unless exists($self->{_bitflu_network}->{$socket});
		return ($self->{_bitflu_network}->{$socket}->{qlen} || 0);
	}
	
	##########################################################################
	# Returns how many bytes we can write to the queue
	sub GetQueueFree {
		my($self,$socket) = @_;
		$self->panic("Cannot return qfree of vanished socket <$socket>") unless exists($self->{_bitflu_network}->{$socket});
		return(MAXONWIRE - $self->GetQueueLen($socket));
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
		
		$self->{_bitflu_network}->{$args{ID}}               = { select => undef, socket => $new_socket, rqi => 0, wqi => 0, config => { MaxPeers=>1, cntMaxPeers=>0, },
		                                                        blacklist => { pointer => 0, array => [], bldb => {}} };
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
		
		$self->{_bitflu_network}->{$args{ID}} = { select => undef, socket => $socket,  rqi => 0, wqi => 0, config => { MaxPeers=>($args{MaxPeers}), cntMaxPeers=>0 },
		                                          blacklist => { pointer => 0, array => [], bldb => {}} };
		$self->{_bitflu_network}->{$args{ID}}->{select}     = new IO::Select or $self->panic("Unable to create new IO::Select object: $!");
		$self->{_bitflu_network}->{$args{ID}}->{listentype} = LT_TCP;
		$self->{_bitflu_network}->{$args{ID}}->{callbacks}  = $args{Callbacks} or $self->panic("Unable to register TCP-Socket without any callbacks");
		$self->{_bitflu_network}->{$args{ID}}->{laddr_in}   = sockaddr_in(0, ($args{Bind} ? inet_aton($args{Bind}) : INADDR_ANY)) or $self->panic("sockaddr failed");
		
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
		
		if($self->{avfds} < 1) {
			return undef; # No Filedescriptors left
		}
		elsif($bfn_strct->{config}->{cntMaxPeers} >= $bfn_strct->{config}->{MaxPeers}) {
			return undef; # Maxpeers reached
		}
		elsif($bfn_strct->{listentype} != LT_TCP) {
			$self->panic("Cannot create TCP connection for socket of type ".$bfn_strct->{listentype}." using $args{ID}");
		}
		
		if(exists($args{Hostname})) {
			# -> Resolve
			my @xresolved = $self->{super}->Tools->Resolve($args{Hostname});
			unless( ($args{Ipv4} = $xresolved[0] ) ) {
				$self->warn("Cannot resolve $args{Hostname}");
				return undef;
			}
		}
		
		if($args{Ipv4} !~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) {
			$self->panic("Invalid IP: $args{Ipv4}");
		}
		
		if($self->IpIsBlacklisted($args{ID}, $args{Ipv4})) {
			$self->debug("Won't connect to blacklisted IP $args{Ipv4}");
			return undef;
		}
		
		my $proto = getprotobyname('tcp');
		my $sock  = undef;
		my $sin   = undef;
		
		socket($sock, AF_INET,SOCK_STREAM,$proto) or $self->panic("Failed to create a new socket : $!");
		bind($sock, $bfn_strct->{laddr_in})       or $self->panic("Failed to bind socket <$sock> to interface : $!");
		eval { $sin = sockaddr_in($args{Port}, inet_aton($args{Ipv4})); };
		
		if(!defined($sin)) {
			$self->warn("Unable to create socket for $args{Ipv4}:$args{Port}");
			return undef;
		}
		
		$self->Unblock($sock) or $self->panic("Failed to unblock new socket <$sock> : $!");
		if(exists($self->{_bitflu_network}->{$sock})) {
			$self->panic("FATAL: DUPLICATE SOCKET-ID <$sock> ?!");
		}
		
		# Write PerSocket information: establishing | outbuff | config
		$bfn_strct->{establishing}->{$sock} = { socket => $sock, till => $self->GetTime+$args{Timeout}, sin => $sin };
		$self->{_bitflu_network}->{$sock}   = { sockmap => $sock, handlemap => $args{ID}, fastwrite => 0, lastio => $self->GetTime, incoming => 0 };
		$self->{avfds}--;
		$bfn_strct->{config}->{cntMaxPeers}++;
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
				elsif($self->{avfds} < 1) {
					$self->warn("System has no file-descriptors left, dropping new incoming connection");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
				}
				elsif(!$self->Unblock($new_sock)) {
					$self->info("Unable to unblock $new_sock : $!");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
				}
				elsif(!($new_ip = $new_sock->peerhost)) {
					$self->debug("Unable to obtain peerhost from $new_sock : $!");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
				}
				elsif($handle_ref->{config}->{cntMaxPeers} >= $handle_ref->{config}->{MaxPeers}) {
					$self->warn("Handle <$handle_id> is full: Dropping new socket");
					$new_sock->close() or $self->panic("Unable to close <$new_sock> : $!");
				}
				elsif($self->IpIsBlacklisted($handle_id, $new_ip)) {
					$self->warn("Refusing incoming connection from blacklisted IP $new_ip");
				}
				else {
					$self->{_bitflu_network}->{$new_sock} = { sockmap => $new_sock, handlemap => $handle_id, fastwrite => 0, lastio => $self->GetTime, incoming => 1 };
					$select_handle->add($new_sock);
					$self->{avfds}--;
					$handle_ref->{config}->{cntMaxPeers}++;
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
				my $new_ip = '';
				my $buffer = undef;
				
				$socket->recv($buffer,POSIX::BUFSIZ); # Read data from socket
				
				if(!($new_ip = $socket->peerhost)) {
					# Weirdo..
					$self->warn("<$socket> had no peerhost, data dropped");
				}
				elsif($self->IpIsBlacklisted($handle_id, $new_ip)) {
					$self->warn("Dropping UDP-Data from blacklisted IP $new_ip");
				}
				elsif(my $cbn = $callbacks->{Data}) {
					$handle_id->$cbn($socket, \$buffer);
				}
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
				$self->debug("Delaying kill of $handle_id -> $socket [Write failed with: $!]");
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
		my $id   = $args{ID}   or $self->panic("No ID given");
		my $data = $args{Data};
		if($self->IpIsBlacklisted($id, $ip)) {
			$self->warn("Won't send UDP-Data to blacklisted IP $ip");
			return undef;
		}
		else {
			my $hisip = IO::Socket::inet_aton($ip);
			my $hispn = IO::Socket::sockaddr_in($port, $hisip);
			my $bs = send($socket,$data,0,$hispn);
			return $bs;
		}
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
		my $queued_bytes = $self->GetQueueLen($socket);
		my $this_bytes   = length($buffer);
		my $total_bytes  = $queued_bytes + $this_bytes;
		my $handle_id    = $self->{_bitflu_network}->{$socket}->{handlemap} or $self->panic("No handleid for $socket ?");
		
		if($total_bytes > MAXONWIRE) {
			$self->warn("<$socket> Buffer overrun! Too much unsent data: $total_bytes bytes");
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
	
	##########################################################################
	# Add an IP to internal blacklist
	sub BlacklistIp {
		my($self, $id, $this_ip) = @_;
		unless($self->IpIsBlacklisted($id, $this_ip)) {
			my $xbl     = $self->{_bitflu_network}->{$id}->{blacklist};
			my $pointer = ( $xbl->{pointer} >= BLIST_LIMIT ? 0 : $xbl->{pointer});
			# Ditch old entry
			my $oldkey = $xbl->{array}->[$pointer];
			defined($oldkey) and delete($xbl->{bldb}->{$oldkey});
			$xbl->{array}->[$pointer] = $this_ip;
			$xbl->{bldb}->{$this_ip}  = $pointer;
			$xbl->{pointer}           = 1+$pointer;
		}
	}
	
	##########################################################################
	# Returns 1 if IP is blacklisted, 0 otherwise
	sub IpIsBlacklisted {
		my($self, $id, $this_ip) = @_;
		# Fixme: Ev. sollten wir nur so für z.B. 30 minuten blacklisten?!
		$self->panic("No id?!") unless $id;
		return (exists($self->{_bitflu_network}->{$id}->{blacklist}->{bldb}->{$this_ip}) ? 1 : 0);
	}
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug("Network : ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("Network : ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("Network : ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("Network : ".$msg); }
	sub stop  { my($self, $msg) = @_; $self->{super}->stop("Network : ".$msg); }

	
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
		my $NOEXEC = '';
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
				if($self->SetValue($key, $value)) { push(@A, [undef, "'$key' set to '$value'"]); $self->Save; }
				else                              { push(@A, [2, "Unable to change value of $key at runtime"]); }
			}
			else {
				push(@A, [2, "Option '$key' does not exist"]);
			}
		}
		else {
			$NOEXEC .= "Usage error, type 'help config' for more information";
		}
		return{MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC};
	}
	
	
	sub Load {
		my($self) = @_;
		$self->SetDefaults();
		if(defined(my $cfh = $self->{configuration_fh})) {
			seek($cfh,0,0) or $self->panic("Unable to seek to beginning");
			my $conf = $self->{super}->Tools->CBxToRef(join('', <$cfh>));
			while(my($k,$v) = each(%$conf)) {
				$self->{conf}->{$k} = $v;
			}
		}
	}
	
	sub SetDefaults {
		my($self) = @_;
		$self->{conf}->{plugindir}       = './plugins';
		$self->{conf}->{pluginexclude}   = '';
		$self->{conf}->{workdir}         = "./workdir";
		$self->{conf}->{tempdir}         = "tmp";
		$self->{conf}->{upspeed}         = 35;
		$self->{conf}->{writepriority}   = 2;
		$self->{conf}->{readpriority}    = 4;
		$self->{conf}->{loglevel}        = 5;
		$self->{conf}->{renice}          = 8;
		$self->{conf}->{sleeper}         = 0.06;
		$self->{conf}->{logfile}         = '';
		$self->{conf}->{history}         = 1;
		$self->{conf}->{default_bind}    = 0;
		$self->{conf}->{storage}         = 'StorageFarabDb';
		foreach my $opt qw(renice plugindir pluginexclude workdir tempdir logfile default_bind storage) {
			$self->RuntimeLockValue($opt);
		}
	}
	
	sub Save {
		my($self) = @_;
		my $cfh = $self->{configuration_fh} or return undef;
		seek($cfh,0,0)   or $self->panic("Unable to seek to beginning");
		truncate($cfh,0) or $self->panic("Unable to truncate configuration file");
		print $cfh $self->{super}->Tools->RefToCBx($self->{conf});
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
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug("Config  : ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("Config  : ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("Config  : ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("Config  : ".$msg); }
	
1;



