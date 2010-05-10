#!/usr/bin/perl -w
#
# This file is part of 'Bitflu' - (C) 2006-2010 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#
use strict;
use Data::Dumper;
use Getopt::Long;
use Danga::Socket;

my $bitflu_run           = undef;     # Start as not_running and not_killed
my $getopts              = { help => undef, config => '.bitflu.config', version => undef, quiet=>undef };
$SIG{PIPE}  = $SIG{CHLD} = 'IGNORE';
$SIG{INT}   = $SIG{HUP}  = $SIG{TERM} = \&HandleShutdown;

GetOptions($getopts, "help|h", "version", "plugins", "config=s", "daemon", "quiet|q") or exit 1;
if($getopts->{help}) {
	die << "EOF";
Usage: $0 [--help --version --plugins --config=s --daemon --quiet]

  -h, --help         print this help.
      --version      display the version of bitflu and exit
      --plugins      list all loaded plugins and exit
      --config=file  use specified configuration file (default: .bitflu.config)
      --daemon       run bitflu as a daemon
  -q, --quiet        disable logging to standard output

Example: $0 --config=/etc/bitflu.config --daemon

Mail bug reports and suggestions to <adrian\@blinkenlights.ch>.
EOF
}


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
elsif($getopts->{quiet}) {
	$bitflu->DisableConsoleOutput;
}

$bitflu->SysinitProcess();
$bitflu->SetupDirectories();
$bitflu->InitPlugins();
$bitflu->PreloopInit();

$bitflu_run = 1 if !defined($bitflu_run); # Enable mainloop and sighandler if we are still not_killed



RunPlugins();
Danga::Socket->SetPostLoopCallback( sub { return $bitflu_run } );
Danga::Socket->EventLoop();


$bitflu->Storage->terminate;
$bitflu->info("-> Shutdown completed after running for ".(int(time())-$^T)." seconds");
exit(0);





sub RunPlugins {
	my $NOW = $bitflu->Network->GetTime;
	foreach my $rk (keys(%{$bitflu->{_Runners}})) {
		my $rx = $bitflu->{_Runners}->{$rk} or $bitflu->panic("Runner $rk vanished");
		next if $rx->{runat} > $NOW;
		$rx->{runat} = $NOW + $rx->{target}->run($NOW);
	}
	if($bitflu_run) {
		Danga::Socket->AddTimer(0.1, sub { RunPlugins() })
	}
}

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
use constant V_MAJOR  => '1';
use constant V_MINOR  => '00';
use constant V_STABLE => 1;
use constant V_TYPE   => ( V_STABLE ? 'stable' : 'devel' );
use constant VERSION  => V_MAJOR.'.'.V_MINOR.'-'.V_TYPE;
use constant APIVER   => 20100424;
use constant LOGBUFF  => 0xFF;

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
		$self->{_Runners}               = {};
		$self->{_Plugins}               = ();
		return $self;
	}
	
	##########################################################################
	# Return internal version
	sub GetVersion {
		my($self) = @_;
		return(V_MAJOR, V_MINOR, V_STABLE);
	}
	
	##########################################################################
	# Return internal version as string
	sub GetVersionString {
		my($self) = @_;
		return VERSION;
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
		$self->{_Runners}->{$target} and $self->panic("$target is registered: wont overwrite it");
		$self->{_Runners}->{$target} = { target=>$target, runat=>0 };
	}
	
	##########################################################################
	# Returns a list of bitflus _Runner array as reference hash
	sub GetRunnerTarget {
		my($self,$target) = @_;
		foreach my $rx (values(%{$self->{_Runners}})) {
			return $rx->{target} if ref($rx->{target}) eq $target;
		}
		return undef;
	}
	
	##########################################################################
	# Creates a new Simple-eXecution task
	sub CreateSxTask {
		my($self,%args) = @_;
		$args{__SUPER_} = $self;
		my $sx = Bitflu::SxTask->new(%args);
		$self->AddRunner($sx);
		#$self->debug("CreateSxTask returns <$sx>");
		#$self->info("SxTask  : ".ref($sx->{super})."->$sx->{cback} created (id: $sx)");
		return $sx;
	}
	
	##########################################################################
	# Kills an SxTask
	sub DestroySxTask {
		my($self,$taskref) = @_;
		delete($self->{_Runners}->{$taskref}) or $self->panic("Could not kill non-existing task $taskref");
		#$self->info("SxTask  : $taskref terminated");
		return 1;
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
		closedir(PLUGINS);
		
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
		my $tmpdir  = $self->Tools->GetTempdir                  or $self->panic("No tempdir configured");
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
		my $chdir  = $self->Configuration->GetValue('chdir');
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
		
		# -> Adjust resolver settings (is this portable? does it work on *BSD?)
		$ENV{RES_OPTIONS} = "timeout:1 attempts:1";
		
		
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
		
		if($chdir) {
			$self->info("Changing into directory '$chdir'");
			chdir($chdir) or $self->stop("chdir($chdir) failed: $!");
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
	
	##########################################################################
	# Set _LogFh to undef if we are logging to stdout
	# this disables logging to the console
	sub DisableConsoleOutput {
		my($self) = @_;
		$self->debug("DisableConsoleOutput called");
		if($self->{_LogFH} eq *STDOUT) {
			$self->debug("=> Setting _LogFH to undef");
			$self->{_LogFH} = '';
		}
		# Do not printout any warnings to STDOUT
		$SIG{__WARN__} = sub {};
	}
	
	##########################################################################
	# Fork into background
	sub Daemonize {
		my($self) = @_;
		my $child = fork();
		
		if(!defined($child)) {
			die "Unable to fork: $!\n";
		}
		elsif($child != 0) {
			$self->debug("Bitflu is running with pid $child");
			exit(0);
		}
		
		$self->DisableConsoleOutput;
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
		my $uptime = ($self->Network->GetTime - $^T)/60;
		return {MSG=>[ [1, sprintf("This is Bitflu %s (API:%s) running on %s with perl %vd. Uptime: %.3f minutes (%s)",$self->GetVersionString,
		                                         APIVER, $^O, $^V, $uptime, "".localtime($^T) )] ], SCRAP=>[]};
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
		
		print $xfh $rmsg if $xfh;
		
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
		$self->yell("IPv6 ?           : ".$self->Network->HaveIPv6);
		$self->yell("Running since    : ".gmtime($^T));
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

use constant SHALEN   => 40;
use constant HPFX     => 'history_';
use constant HIST_MAX => 100;
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
		my $toload   = int(@$queueIds);
		$self->info("Resuming $toload downloads, this may take a few seconds...");
		
		foreach my $sid (@$queueIds) {
			my $this_storage = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage for sid $sid");
			my $owner        = $this_storage->GetSetting('owner');
			
			$self->info(sprintf("[%3d] Loading %s", $toload--, $sid));
			
			if(defined($owner) && (my $r_target = $self->{super}->GetRunnerTarget($owner)) ) {
				$r_target->resume_this($sid);
			}
			else {
				$self->stop("StorageObject $sid is owned by '$owner', but plugin is not loaded/registered correctly");
			}
		}
		
		$self->{super}->Admin->RegisterCommand('rename'  , $self, 'admincmd_rename', 'Renames a download',
		         [ [undef, "Renames a download"], [undef, "Usage: rename queue_id \"New Name\""] ]);
		$self->{super}->Admin->RegisterCommand('cancel'  , $self, 'admincmd_cancel', 'Removes a file from the download queue',
		         [ [undef, "Removes a file from the download queue"], [undef, "Usage: cancel queue_id [queue_id2 ... | --all]"] ]);
		
		$self->{super}->Admin->RegisterCommand('history' , $self, 'admincmd_history', 'Manages download history',
		        [  [undef, "Manages internal download history"], [undef, ''],
		           [undef, "Usage: history [ queue_id [show forget] ] [list|drop|cleanup]"], [undef, ''],
		           [undef, "history list            : List all remembered downloads"],
		           [undef, "history drop            : List and forget all remembered downloads"],
		           [undef, "history cleanup         : Trim history size"],
		           [undef, "history queue_id show   : Shows details about queue_id"],
		           [undef, "history queue_id forget : Removes history of queue_id"],
		        ]);
		
		$self->{super}->Admin->RegisterCommand('pause' , $self, 'admincmd_pause', 'Stops a download',
		        [  [undef, "Stop given download"], [undef, ''],
		           [undef, "Usage: pause queue_id [queue_id2 ... | --all]"], [undef, ''],
		        ]);
		
		$self->{super}->Admin->RegisterCommand('resume' , $self, 'admincmd_resume', 'Resumes a paused download',
		        [  [undef, "Resumes a paused download"], [undef, ''],
		           [undef, "Usage: resume queue_id [queue_id2 ... | --all]"], [undef, ''],
		        ]);
		
		$self->info("--- startup completed: bitflu ".$self->{super}->GetVersionString." is ready ---");
		return 1;
	}
	
	
	##########################################################################
	# Pauses a download
	sub admincmd_pause {
		my($self, @args) = @_;
		
		my @MSG    = ();
		my $NOEXEC = '';
		$self->{super}->Tools->GetOpts(\@args);
		
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
		$self->{super}->Tools->GetOpts(\@args);
		
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
		
		my @MSG     = ();
		my $NOEXEC  = '';
		$self->{super}->Tools->GetOpts(\@args);
		
		if(int(@args)) {
			foreach my $cid (@args) {
				my $storage = $self->{super}->Storage->OpenStorage($cid);
				if($storage) {
					my $owner = $storage->GetSetting('owner');
					if(defined($owner) && (my $r_target = $self->{super}->GetRunnerTarget($owner)) ) {
						$self->ModifyHistory($cid, Canceled=>'');
						$r_target->cancel_this($cid);
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
		
		if($sha eq 'list' or $sha eq 'drop' or $sha eq 'cleanup') {
			my @cbl    = $strg->ClipboardList;
			my $cbi    = 0;
			my $drop   = {};
			my $drop_c = 0;
			
			foreach my $item (@cbl) {
				if(my($this_sid) = $item =~ /^$hpfx(.+)$/) {
					my $hr = $self->GetHistory($this_sid);      # History Reference
					my $ll = "$1 : ".substr(($hr->{Name}||''),0,64);  # Telnet-Safe-Name
					push(@MSG, [ ($strg->OpenStorage($this_sid) ? 1 : 5 ), $ll]);
					$strg->ClipboardRemove($item)                     if $sha eq 'drop';
					push(@{$drop->{($hr->{FirstSeen}||0)}},$this_sid) if $sha eq 'cleanup'; # Create list with possible items to delete
					$cbi++;
				}
			}
			
			
			# Walk droplist (if any). From oldest to newest
			foreach my $d_sid ( map(@{$drop->{$_}}, sort({$a<=>$b} keys(%$drop))) ) {
				next if $strg->OpenStorage($d_sid);      # do not even try to drop existing items (wouldn't do much harm but...)
				last if ( $cbi-$drop_c++ ) <= HIST_MAX;  # Abort if we reached our limit
				$strg->ClipboardRemove(HPFX.$d_sid);     # Still here ? -> ditch it. (fixme: HPFX concating is ugly)
			}
			
			
			push(@MSG, [1, ($sha eq 'drop' ? "History cleared" : "$cbi item".($cbi == 1 ? '' : 's')." stored in history")]);
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
				                               Ended=>'never', Committed=>'never', FirstSeen=>$self->{super}->Network->GetTime);
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
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug("QueueMGR: ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("QueueMGR: ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("QueueMGR: ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("QueueMGR: ".$msg); }
	sub stop  { my($self, $msg) = @_; $self->{super}->stop("QueueMGR: ".$msg);  }

1;

###############################################################################################################
# Bitflu Sammelsurium
package Bitflu::Tools;

	use MIME::Base64 ();
	
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
			elsif($line =~ /^([a-zA-Z0-9_:\.]+)\s*=\s*(.*)$/) {
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
			$key =~ tr/a-zA-Z0-9_:\.//cd;
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
		opendir(my $DFH, $pfx);
		foreach my $dirent (readdir($DFH)) {
			my $fp = "$pfx/".$dirent;
			next if $dirent eq '.';   # No thanks
			next if $dirent eq '..';  # Ditto
			next if (-l $fp);         # Won't look at symlinks
			push(@{$dstruct->{list}},$fp);
			$self->GenDirList($dstruct,$dirent) if -d $fp;
		}
		closedir($DFH);
		pop(@{$dstruct->{_}});
	}
	
	##########################################################################
	# Getopts like support
	sub GetOpts {
		my($self,$args) = @_;
		my @leftovers = ();
		my $ctx       = undef;
		my $argref    = {};
		my $getargs   = 1;
		foreach my $this_arg (@$args) {
			
			if($getargs) {
				if($this_arg eq '--') {
					$getargs = 0;
				}
				elsif($this_arg eq '--all') {
					my $ql = $self->{super}->Queue->GetQueueList;
					foreach my $protocol (keys(%$ql)) {
						push(@leftovers, keys(%{$ql->{$protocol}}));
					}
				}
				elsif($this_arg =~ /^--?(.+)/) {
					$ctx = $1;
					$argref->{$ctx} = 1 if !exists $argref->{$ctx};
				}
				elsif(defined($ctx)) {
					$argref->{$ctx} = $this_arg;
					$ctx = undef;
				}
				else {
					push(@leftovers, $this_arg);
				}
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
	
	##########################################################################
	# Return exclusive name for a file
	sub GetExclusivePath {
		my($self, $basedir) = @_;
		my $dest = '';
		my $i    = 0;
		while(1) {
			$dest = sprintf("%s/%x-%x-%x.tmp_$i", $basedir, $$, int(rand(0xFFFFFF)), int(time()));
			return $dest if !(-e $dest);
		}
		return undef;
	}
	
	##########################################################################
	# Return path to non-existing file within tempdir
	sub GetExclusiveTempfile {
		my($self) = @_;
		return $self->GetExclusivePath($self->GetTempdir);
	}
	
	
	##########################################################################
	# Return temp directory
	sub GetTempdir {
		my($self) = @_;
		return $self->{super}->Configuration->GetValue('workdir')."/tmp";
	}
	
	##########################################################################
	# looping sysread implementation
	# *BSD doesn't like big LENGTH values on sysread
	# This provides a crappy warper to 'fix' this problem
	# syswrite() doesn't seem to suffer the same problem ...
	sub Sysread {
		my($self, $fh, $ref, $bytes_needed) = @_;
		
		my $bytes_left = $bytes_needed;
		my $buff       = '';
		
		$self->panic("Cannot read $bytes_needed bytes") if $bytes_needed < 0;
		while($bytes_left > 0) {
			my $br = sysread($fh, $buff, $bytes_left);
			if($br)             { ${$ref} .= $buff; $bytes_left -= $br; } # Data
			elsif(defined($br)) { last;                                 } # EOF
			else                { return undef;                         } # Error
		}
		return ($bytes_needed-$bytes_left);
	}
	
	########################################################################
	# Decodes Compact IP-Chunks
	sub DecodeCompactIp {
		my($self, $compact_list) = @_;
		my @peers = ();
			for(my $i=0;$i<length($compact_list);$i+=6) {
				my $chunk = substr($compact_list, $i, 6);
				my($a,$b,$c,$d,$port) = unpack("CCCCn", $chunk);
				my $ip = "$a.$b.$c.$d";
				push(@peers, {ip=>$ip, port=>$port, peer_id=>""});
			}
		return @peers;
	}

	########################################################################
	# Decodes IPv6 Chunks
	sub DecodeCompactIpV6 {
		my($self, $compact_list) = @_;
		my @peers = ();
			for(my $i=0;$i<length($compact_list);$i+=18) {
				my $chunk = substr($compact_list, $i, 18);
				my(@sx)   = unpack("nnnnnnnnn", $chunk);
				my $port  = pop(@sx);
				my $ip    = join(':',map(sprintf("%x", $_),@sx)); # Must match ExpandIpv6, otherwise babies will cry and cats might even die.
				push(@peers, {ip=>$ip, port=>$port, peer_id=>""});
			}
		return @peers;
	}
	
	################################################################################################
	# Stolen from http://www.stonehenge.com/merlyn/UnixReview/col30.html
	sub DeepCopy {
		my($self,$this) = @_;
		if (not ref $this) {
			$this;
		} elsif (ref $this eq "ARRAY") {
			[map $self->DeepCopy($_), @$this];
		} elsif (ref $this eq "HASH") {
			+{map { $_ => $self->DeepCopy($this->{$_}) } keys %$this};
		} else { Carp::confess "what type is $_?" }
	}
	
	################################################################################################
	# Decode bencoded data
	sub BencDecode {
		return Bitflu::Bencoder::decode($_[1]);
	}
	
	################################################################################################
	# Serialize bencoded data
	sub BencEncode {
		return Bitflu::Bencoder::encode($_[1]);
	}
	
	################################################################################################
	# Load file from disc and return both raw+decoded data
	sub BencfileToHash {
		my($self,$file) = @_;
		
		open(BENC, "<", $file) or return {};
		my $buff = join('',<BENC>);
		close(BENC);
		return {} if (!defined($buff) or length($buff)==0); # File too short
		
		my $decoded = $self->BencDecode($buff);
		return {} if ref($decoded) ne 'HASH';               # Decoding failed
		
		return { content=>$decoded, raw_content=>$buff };   # All ok!
	}
	
	################################################################################################
	# Guess eta for given hash
	sub GetETA {
		my($self, $key) = @_;
		
		my $so      = $self->{super}->Storage->OpenStorage($key) or return undef;
		my $created = $so->GetSetting('createdat')               or return undef;
		my $stats   = $self->{super}->Queue->GetStats($key);
		my $age     = time()-$created;
		
		if($age > 0) {
			my $not_done = $stats->{total_bytes} - $stats->{done_bytes};
			my $bps      = $stats->{done_bytes} / $age;
			   $bps      = $stats->{speed_download} if $stats->{speed_download} > $bps; # we are optimistic ;-)
			my $eta_sec  = ( $bps > 0 ? ($not_done / $bps) : undef );
			return $eta_sec;
		}
		# else
		return undef;
	}
	
	################################################################################################
	# Convert number of seconds into something for humans
	sub SecondsToHuman {
		my($self,$sec) = @_;
		
		if(!defined($sec)) {
			return 'inf.';
		}
		elsif($sec < 5) {
			return '-';
		}
		elsif($sec < 60) {
			return int($sec)." sec";
		}
		elsif($sec < 60*90) {
			return int($sec/60)."m";
		}
		elsif($sec < 3600*48) {
			return sprintf("%.1fh", $sec/3600);
		}
		elsif($sec < 86400*10) {
			return sprintf("%.1fd", $sec/86400);
		}
		elsif($sec < 86400*31) {
			return sprintf("%.1fw", $sec/86400/7);
		}
		else {
			return ">4w"
		}
		
	}
	
	sub warn   { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub debug  { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg);  }
	sub stop   { my($self, $msg) = @_; $self->{super}->stop(ref($self).": ".$msg); }

1;


###############################################################################################################
# Bitflu Admin-Dispatcher : Release 20070319_1
package Bitflu::Admin;

	##########################################################################
	# Guess what?
	sub new {
		my($class, %args) = @_;
		my $self = {super=> $args{super}, cmdlist => {}, notifylist => {}, complist=>[] };
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
		
		
		my @A = ([1, "Known plugins:"]);
		
		foreach my $pref (@{$self->{super}->{_Plugins}}) {
			push(@A, [undef, " ".$pref->{package}]);
		}
		
		push(@A, [undef,''],[1,"Scheduler jobs:"]);
		foreach my $rref (values(%{$self->{super}->{_Runners}})) {
			push(@A, [undef, " ".$rref->{target}]);
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
	# Register as completion service
	sub RegisterCompletion {
		my($self,$xref,$xcmd) = @_;
		$self->debug("RegisterCompletition: Will ask $xref->$xcmd for completion");
		push(@{$self->{complist}}, { class=>$xref, cmd=>$xcmd });
	}
	
	##########################################################################
	# Callss all completition services and returns the result
	sub GetCompletion {
		my($self,$hint) = @_;
		my @result = ();
		foreach my $xref (@{$self->{complist}}) {
			my $class = $xref->{class} or $self->panic("$xref has no class!");
			my $cmd   = $xref->{cmd}   or $self->panic("$xref has no cmd!");
			my @this  = $class->$cmd($hint);
			push(@result,@this);
		}
		return @result;
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
# Bitflu Network-IO Lib : Release 20090125_1
package Bitflu::Network;

use strict;
use IO::Socket;
use POSIX;
use Danga::Socket;
use Hash::Util; # For chroot

use constant DEVNULL      => '/dev/null';   # Path to /dev/null
use constant MAXONWIRE    => 1024*1024;     # Do not buffer more than 1mb per client connection
use constant BF_BUFSIZ    => 327680;         # How much we shall read()/recv() from a socket per run
use constant NI_SIXHACK   => 3;
use constant BPS_MIN      => 8;             # Minimal upload speed per socket
use constant NETSTATS     => 2;             # ReGen netstats each 2 seconds
use constant NETDEBUG     => 0;
use constant BLIST_LIMIT  => 1024;          # NeverEver blacklist more than 1024 IPs per instance
use constant BLIST_TTL    => 60*60;         # BL entries are valid for 1 hour
use constant DNS_BLIST    => 5;             # How long shall we blacklist 'bad' dns entries (NOTE: DNS_BLIST**rowfail !)
use constant DNS_BLTTL    => 60;            # Purge any older DNS-Blacklist entries

use fields qw( super NOWTIME avfds bpc _HANDLES _SOCKETS stats resolver_fail );

my $HAVE_IPV6 = 0;
	
	##########################################################################
	# Creates a new Networking Object
	sub new {
		my($class, %args) = @_;
		my $ptype = {super=> $args{super}, NOWTIME => 0, avfds => 0, bpc=>BPS_MIN, _HANDLES=>{}, _SOCKETS=>{},
		             stats => {nextrun=>0, sent=>0, recv=>0, raw_recv=>0, raw_sent=>0}, resolver_fail=>{} };
		
		my $self = fields::new($class);
		map( $self->{$_} = delete($ptype->{$_}), keys(%$ptype) );
		
		$self->SetTime;
		$self->{avfds} = $self->TestFileDescriptors;
		$self->debug("Reserved $self->{avfds} file descriptors for networking");
		
		if($self->{super}->Configuration->GetValue('ipv6')) {
			eval "use Danga::Socket 1.61; 1; "; # Check if at least 1.61 is installed
			if($@) {
				$self->warn("Danga::Socket 1.61 is required for IPv6 support.");
				$self->warn("Disabling IPv6 due to outdated Danga::Socked version");
			}
			else {
				eval {
					require IO::Socket::INET6;
					require Socket6;
					$HAVE_IPV6 = 1;
					
					if( (my $isiv = $IO::Socket::INET6::VERSION) < 2.56 ) {
						$self->warn("Detected outdated version of IO::Socket::INET6 ($isiv)");
						$self->warn("Please upgrade to IO::Socket::INET6 >= 2.56 !");
						$self->warn("IPv6 might not work correctly with version $isiv");
					}
					
				};
			}
		}
		return $self;
	}
	
	##########################################################################
	# Register Admin commands
	sub init {
		my($self) = @_;
		$self->SetTime;
		$self->info("IPv6 support is ".($self->HaveIPv6 ? 'enabled' : 'not active'));
		
		$self->{super}->AddRunner($self);
		$self->{super}->Admin->RegisterCommand('blacklist', $self, '_Command_Blacklist', 'Display current in-memory blacklist');
		$self->{super}->Admin->RegisterCommand('netstat',   $self, '_Command_Netstat',   'Display networking statistics');
		$self->{super}->Admin->RegisterCommand('dig',       $self, '_Command_Dig',       'Resolve a hostname');
		
		return 1;
	}
	
	sub run {
		my($self) = @_;
		$self->SetTime;
		$self->_Throttle;
		return 0; # Cannot use '1' due to deadlock :-)
	}
	
	##########################################################################
	# Resolver debug
	sub _Command_Dig {
		my($self, $hostname) = @_;
		my @A = ();
		if($hostname) {
			push(@A, [1, "Resolver result for '$hostname'"]);
			foreach my $entry ($self->Resolve($hostname)) {
				push(@A, [0, "  $entry"]);
			}
		}
		else {
			push(@A, [2, "Usage: dig hostname"]);
		}
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	##########################################################################
	# Display blacklist information
	sub _Command_Blacklist {
		my($self) = @_;
		
		my @A = ();
		foreach my $this_handle (keys(%{$self->{_HANDLES}})) {
			push(@A, [4, "Blacklist for '$this_handle'"]);
			my $count = 0;
			while( my($k,$v) = each(%{$self->{_HANDLES}->{$this_handle}->{blacklist}->{bldb}}) ) {
				my $this_ttl = $v - $self->GetTime;
				next if $this_ttl <= 0;
				push(@A, [2, sprintf(" %-24s (expires in %d seconds)", $k, $this_ttl) ]);
				$count++;
			}
			push(@A, [3, "$count ip(s) are blacklisted"], [undef, '']);
		}
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	##########################################################################
	# Netstat command
	sub _Command_Netstat {
		my($self) = @_;
		
		my @A = ();
		
		my $sock_to_handle = {};
		map($sock_to_handle->{$_}=0           ,keys(%{$self->{_HANDLES}}));
		map($sock_to_handle->{$_->{handle}}++, values(%{$self->{_SOCKETS}}));
		
		push(@A, [4, "Socket information"]);
		foreach my $this_handle (sort keys(%$sock_to_handle)) {
			my $hxref = $self->{_HANDLES}->{$this_handle};
			push(@A, [3, " Statistics for '$this_handle'"]);
			push(@A, [1, sprintf("  %-24s : %s", "Free sockets", (exists($hxref->{avpeers}) ? sprintf("%3d",$hxref->{avpeers}) : '  -') ) ]);
			push(@A, [1, sprintf("  %-24s : %3d", "Used sockets", $sock_to_handle->{$this_handle}) ]);
		}
		
		push(@A, [0, '-' x 60]);
		push(@A, [0, sprintf(" >> Total: used=%d / watched=%d / free=%d",int(keys(%{$self->{_SOCKETS}})), Danga::Socket->WatchedSockets(), $self->{avfds} )]);
		
		push(@A, [0,''],[4, "Resolver fail-list"]);
		while(my($k,$r) = each(%{$self->{resolver_fail}})) {
			push(@A, [2, sprintf(" %-32s : Row-Fails=>%3d, FirstFail=>%s", $k, $r->{rfail}, "".localtime($r->{first_fail}))]);
		}
		
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	
	
	##########################################################################
	# Test how many filedescriptors this OS / env can handle
	sub TestFileDescriptors {
		my($self)   = @_;
		my $i       = 0;
		my @fdx     = ();
		my $sysr    = 32; # Reserve 32 FDs for OS & co.
		my $canhave = 0;
		
		open(FAKE, DEVNULL) or $self->stop("Unable to open ".DEVNULL.": $!");
		close(FAKE);
		
		while($i++ < 2048) { last unless( open($fdx[$i], DEVNULL) ); }
		if($i > $sysr) { $canhave = $i - $sysr; }
		else           { $self->panic("Sorry, bitfu can not run with only $i filedescriptors left"); }
		
		while(--$i > 0) {
			close($fdx[$i]) or $self->panic("Unable to close TestFD # $i : $!");
		}
		
		return $canhave;
	}
	
	##########################################################################
	# Resolve hostnames
	sub Resolve {
		my($self,$name) = @_;
		my @iplist = ();
		
		if($self->IsValidIPv4($name) or $self->IsValidIPv6($name)) {
			# Do not even try to resolve things looking like IPs
			push(@iplist,$name);
		}
		else {
			my $NOWTIME = $self->GetTime;                     # Current time guess
			my $blref   = $self->{resolver_fail}->{$name};    # Refernce to blacklist entry for this name (can be undef)
			my $is_bad  = 0;                                  # true = do not try to resolve / false = call resolver
			
			if($blref && ($blref->{first_fail}+(DNS_BLIST**$blref->{rfail})) > $NOWTIME) {
				# -> We got an entry and it is still makred as a fail: skip resolver
				$self->warn("Resolve: won't resolve blacklisted DNS-Entry '$name'");
			}
			else {
				if($self->HaveIPv6) {
					my @addr_info = Socket6::getaddrinfo($name, defined);
					for(my $i=0;$i+3<int(@addr_info);$i+=5) {
						my ($addr,undef) = Socket6::getnameinfo($addr_info[$i+3], NI_SIXHACK);
						push(@iplist,$addr);
					}
				}
				else {
					my @result = gethostbyname($name);
					@iplist = map{ inet_ntoa($_) } @result[4..$#result];
				}
				
				# Take care of resolver_fail:
				if(int(@iplist)==0) {
					# -> No A records? -> Mark entry as failed:
					$self->{resolver_fail}->{$name} ||= { first_fail=>$NOWTIME, rfail=>0 };  # Creates a new reference if empty...
					$self->{resolver_fail}->{$name}->{rfail}++;                              # Adjust row-fail count (used for timeout calculation)
					
					# purge old entries (FIXME: can we use map?)
					while(my($xname,$xref)=each(%{$self->{resolver_fail}})) {
						delete($self->{resolver_fail}->{$xname}) if $xref->{first_fail}+DNS_BLTTL < $NOWTIME;
					}
					
				}
				else {
					# -> Lookup was okay: delete fail-entry (if any)
					delete($self->{resolver_fail}->{$name});
				}
			}
		}
		
		return List::Util::shuffle(@iplist);
	}
	
	##########################################################################
	# Returns IP sorted by protocol
	sub ResolveByProto {
		my($self,$name) = @_;
		
		my @iplist = $self->Resolve($name);
		my $list   = { 4=>[], 6=>[] };
		foreach my $ip (@iplist) {
			if($self->IsValidIPv4($ip))  { push(@{$list->{4}},$ip) }
			if($self->IsNativeIPv6($ip)) { push(@{$list->{6}},$ip) }
		}
		return $list;
	}
	
	##########################################################################
	# Emulates getaddrinfo() in ipv6 mode
	sub _GetAddrFoo {
		my($self,$ip,$port,$af,$rqproto) = @_;
		
		my ($family,$socktype,$proto,$sin) = undef;
		if($self->HaveIPv6) {
			$socktype = ($rqproto eq 'tcp' ? SOCK_STREAM : ($rqproto eq 'udp' ? SOCK_DGRAM : $self->panic("Invalid proto: $proto")) );
			($family, $socktype, $proto, $sin) = Socket6::getaddrinfo($ip,$port,$af,$socktype);
		}
		else {
			$family   = IO::Socket::AF_INET;
			$socktype = ($rqproto eq 'tcp' ? SOCK_STREAM : ($rqproto eq 'udp' ? SOCK_DGRAM : $self->panic("Invalid proto: $rqproto")) );
			$proto    = getprotobyname($rqproto);
			eval { $sin      = sockaddr_in($port,inet_aton($ip)); };
		}
		return($family,$socktype,$proto,$sin);
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
	# Returns TRUE if we are running with IPv6 support
	sub HaveIPv6 {
		return $HAVE_IPV6;
	}
	
	##########################################################################
	# Returns TRUE if we are running with IPv4 support (always?);
	sub HaveIPv4 {
		return 1;
	}
	
	##########################################################################
	# Returns TRUE if given string represents a valid IPv4 peeraddr
	sub IsValidIPv4 {
		my($self,$str) = @_;
		if(defined($str) && $str =~ /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/) {
			return 1;
		}
		return 0;
	}
	
	##########################################################################
	# Returns TRUE if given string represents a valid IPv6 peeraddr
	sub IsValidIPv6 {
		my($self,$str) = @_;
		if(defined($str) && $str =~ /^[a-f0-9:]+$/i) { # This is a very BAD regexp..
			return 1;
		}
		return 0;
	}
	
	##########################################################################
	# Convert Pseudo-IPv6 to real IPv4
	sub SixToFour {
		my($self,$str) = @_;
		if($str =~ /^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/) {
			return $1;
		}
		return undef;
	}
	
	##########################################################################
	# Returns TRUE if given IP is a 'real' IPv6 IP
	sub IsNativeIPv6 {
		my($self,$str) = @_;
		
		if($self->IsValidIPv6($str) && !$self->SixToFour($str)) {
			return 1;
		}
		return 0;
	}
	
	
	########################################################################
	# 'expands' a shorted IPv6 using sloppy code
	sub ExpandIpV6 {
		my($self,$ip) = @_;
		my $addrlen = 8;
		my @buff    = (0,0,0,0,0,0,0,0);
		my @ipend   = ();
		my $cnt     = 0;
		my $drp     = undef;
		foreach my $item (split(':',$ip)) {
			if(!defined $drp) {
				( $item eq '' ? ($drp=$cnt): ($buff[$cnt] = hex($item)) );
			}
			else {
				push(@ipend, hex($item));
			}
			last if ++$cnt == $addrlen;
		}
		
		if(defined($drp)) {
			# We got some piggyback data:
			my $e_len  = int(@ipend);       # Number of items
			my $offset = $addrlen-$e_len;   # Offset to use
			if($offset >= 0) {
				for($offset..($addrlen-1)) {
					$buff[$_] = shift(@ipend);
				}
			}
		}
		return ( wantarray ? (@buff) : (join(':',map(sprintf("%x",$_),@buff))) ); # Do not change the sprintf() call as it must be consinstent with DecodeCompactIpV6
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
	# Try to create a new listening socket
	# NewTcpListen(ID=>UniqueueRunnerId, Port=>PortToListen, Bind=>IPv4ToBind, Callbacks => {})
	sub NewTcpListen {
		my($self,%args) = @_;
		
		my $handle_id = $args{ID} or $self->panic("No Handle ID?");
		my $maxpeers  = $args{MaxPeers};
		my $port      = $args{Port};
		my $bindto    = $args{Bind};
		my $cbacks    = $args{Callbacks};
		my $socket    = 0;
		
		if(exists($self->{_HANDLES}->{$handle_id})) {
			$self->panic("Cannot register multiple versions of handle_id $handle_id");
		}
		elsif($maxpeers < 1) {
			$self->panic("$handle_id: MaxPeers cannot be < 1 (is: $maxpeers)");
		}
		elsif($port) {
			my %sargs = (LocalPort=>$port, LocalAddr=>$bindto, Proto=>'tcp', ReuseAddr=>1, Listen=>1024);
			$socket = ( $self->HaveIPv6 ? IO::Socket::INET6->new(%sargs) : IO::Socket::INET->new(%sargs) ) or return undef;
		}
		
		$self->{_HANDLES}->{$handle_id} = { lsock => $socket, cbacks=>$cbacks, avpeers=>$maxpeers, blacklist=>{pointer=>0,array=>[],bldb=>{}} };
		
		if($socket) {
			my $dsock = Bitflu::Network::Danga->new(sock=>$socket, on_read_ready => sub { $self->_TCP_Accept(shift) } ) or $self->panic;
			$self->{_SOCKETS}->{$socket} = { dsock => $dsock, handle=>$handle_id };
		}
		
		return $socket;
	}
	
	sub NewUdpListen {
		my($self,%args) = @_;
		my $handle_id = $args{ID} or $self->panic("No Handle ID?");
		my $port      = $args{Port};
		my $bindto    = $args{Bind};
		my $cbacks    = $args{Callbacks};
		my $socket    = 0;
		
		if(exists($self->{_HANDLES}->{$handle_id})) {
			$self->panic("Cannot register multiple versions of handle_id $handle_id");
		}
		elsif($port) {
			my %sargs = (LocalPort=>$port, LocalAddr=>$bindto, Proto=>'udp');
			$socket = ( $self->HaveIPv6 ? IO::Socket::INET6->new(%sargs) : IO::Socket::INET->new(%sargs) ) or return undef;
		}
		
		$self->{_HANDLES}->{$handle_id} = { lsock => $socket, cbacks=>$cbacks, blacklist=>{pointer=>0,array=>[],bldb=>{}} };
		
		if($socket) {
			my $dsock = Bitflu::Network::Danga->new(sock=>$socket, on_read_ready => sub { $self->_UDP_Read(shift) } ) or $self->panic;
			$self->{_SOCKETS}->{$socket} = { dsock => $dsock, handle=>$handle_id };
		}
		
		return $socket;
	}
	
	sub SendUdp {
		my($self, $socket, %args) = @_;
		my $ip   = $args{RemoteIp} or $self->panic("No IP given");
		my $port = $args{Port}     or $self->panic("No Port given");
		my $id   = $args{ID}       or $self->panic("No ID given");
		my $data = $args{Data};
		
		if($self->IpIsBlacklisted($id, $ip)) {
			$self->debug("Won't send UDP-Data to blacklisted IP $ip");
			return undef;
		}
		else {
			my @af  = $self->_GetAddrFoo($ip,$port,AF_UNSPEC,'udp');
			my $sin = $af[3] or return undef;
			my $bs  = send($socket,$data,0,$sin);
			return $bs;
		}
	}
	
	sub RemoveSocket {
		my($self,$handle_id,$sock) = @_;
		$self->debug("RemoveSocket($sock)") if NETDEBUG;
		my $sref = delete($self->{_SOCKETS}->{$sock}) or $self->panic("$sock was not registered?!");
		my $hxref= $self->{_HANDLES}->{$handle_id}    or $self->panic("No handle reference for $handle_id !");
		$sref->{dtimer}->cancel if $sref->{dtimer};
		$sref->{dsock}->close;
		$self->{avfds}++;
		$hxref->{avpeers}++;
		return 1;
	}
	
	sub WriteDataNow {
		my($self,$sock,$data) = @_;
		return $self->_WriteReal($sock,$data,1,0);
	}
	
	sub WriteData {
		my($self,$sock,$data) = @_;
		return $self->_WriteReal($sock,$data,0,0);
	}
	
	sub _WriteReal {
		my($self,$sock,$data,$fast,$timed) = @_;
		
		my $sref         = $self->{_SOCKETS}->{$sock} or $self->panic("$sock has no _SOCKET entry!");
		my $this_len     = length($data);
		
		
		if($self->GetQueueLen($sock) > MAXONWIRE) {
			$self->warn("Buffer overrun for <$sock>: Too much unsent data!");
			return 1;
		}
		
		
		$sref->{writeq} .= $data;
		$sref->{qlen}   += $this_len;
		
		
		
		if($timed) {
			$sref->{dtimer} = undef; # Clear old timer (it just fired itself);
		}
		
		if($sref->{dtimer}) {
			# -> Still waiting for a timer
		}
		else {
			my $timr     = 0.05;
			
			if($sref->{dsock}->{write_buf_size} == 0) {
				# Socket is empty
				my $bpc            = ($fast? BF_BUFSIZ : $self->{bpc});
				my $sendable       = ($sref->{qlen} < $bpc ? $sref->{qlen} : $bpc );
				my $chunk          = substr($sref->{writeq},0,$sendable);
				$sref->{writeq}    = substr($sref->{writeq},$sendable);
				$sref->{qlen}     -= $sendable;
				$self->{stats}->{raw_sent} += $sendable;
				# Actually write data:
				$sref->{dsock}->write(\$chunk);
				
				$self->debug("$sock has $sref->{qlen} bytes outstanding (sending: $sendable :: $fast :: $timed) ") if NETDEBUG;
				
				unless($sref->{dsock}->sock) {
					$self->debug("$sock went away while writing to it ($!) , scheduling kill timer");
					# Fake a 'connection timeout' -> This goes trough the whole kill-chain so it should be save
					Danga::Socket->AddTimer(0, sub { $self->_TCP_LazyClose($sref->{dsock},$sock); });
				}
				
			}
			else {
				$timr = ( $fast ? 0.05 : 1 );
			}
			
			if($self->GetQueueLen($sock)) {
				$sref->{dtimer} = Danga::Socket->AddTimer($timr, sub { $self->_WriteReal($sock,'',$fast,1); });
			}
		}
		
		return 1;
	}
	
	
	##########################################################################
	# Returns TRUE if socket is an INCOMING connection
	sub IsIncoming {
		my($self,$socket) = @_;
		my $val = $self->{_SOCKETS}->{$socket}->{incoming};
		$self->panic("$socket has no incoming value!") unless defined($val);
		return $val;
	}
	
	##########################################################################
	# Returns last IO for given socket
	sub GetLastIO {
		my($self,$socket) = @_;
		my $val = $self->{_SOCKETS}->{$socket}->{lastio};
		$self->panic("$socket has no lastio value!") unless defined($val);
		return $val;
	}
	
	sub GetQueueFree {
		my($self,$sock) = @_;
		my $xfree = ((MAXONWIRE)-$self->GetQueueLen($sock));
		return ((MAXONWIRE)-$self->GetQueueLen($sock));
	}
	
	sub GetQueueLen {
		my($self,$sock) = @_;
		my $sref = $self->{_SOCKETS}->{$sock} or $self->panic("$sock has no _SOCKET entry!");
		return $sref->{dsock}->{write_buf_size}+$sref->{qlen};
	}
	
	sub NewTcpConnection {
		my($self,%args) = @_;
		
		my $handle_id = $args{ID}                       or $self->panic("No Handle ID?");
		my $hxref     = $self->{_HANDLES}->{$handle_id} or $self->panic("No Handle reference for $handle_id");
		my $remote_ip = $args{RemoteIp};
		my $port      = $args{Port};
		my $new_sock  = undef;
		
		if($self->{avfds} < 1) {
			return undef; # No more FDs :°-(
		}
		elsif($hxref->{avpeers} < 1) {
			return undef; # Handle is full
		}
		
		if(exists($args{Hostname})) {
			# -> Resolve
			my @xresolved = $self->Resolve($args{Hostname});
			unless( ($remote_ip = $xresolved[0] ) ) {
				$self->warn("Cannot resolve $args{Hostname}");
				return undef;
			}
		}
		
		if($self->IpIsBlacklisted($handle_id,$remote_ip)) {
			$self->debug("Won't connect to blacklisted IP $remote_ip");
			return undef;
		}
		
		my($sx_family, $sx_socktype, $sx_proto, $sin) = $self->_GetAddrFoo($remote_ip,$port,AF_UNSPEC, 'tcp');
		
		if(defined($sin)) {
			socket($new_sock, $sx_family, $sx_socktype, $sx_proto) or $self->panic("Failed to create IPv6 Socket: $!");
			$self->Unblock($new_sock) or $self->panic("Failed to unblock <$new_sock> : $!");
			connect($new_sock,$sin);
		}
		else {
			$self->warn("Unable to create socket for $remote_ip:$port");
			return undef;
		}
		my $new_dsock = Bitflu::Network::Danga->new(sock=>$new_sock, on_read_ready => sub { $self->_TCP_Read(shift); }) or $self->panic;
		$self->{_SOCKETS}->{$new_sock} = { dsock => $new_dsock, peerip=>$remote_ip, handle=>$handle_id, incoming=>0, lastio=>$self->GetTime, writeq=>'', qlen=>0, dtimer=>undef };
		$self->{avfds}--;
		$hxref->{avpeers}--;
		$self->debug("<< $new_dsock -> $remote_ip ($new_sock)") if NETDEBUG;
		Danga::Socket->AddTimer(15, sub { $self->_TCP_LazyClose($new_dsock,$new_sock)  });
		
		return $new_sock;
	}
	
	sub _TCP_LazyClose {
		my($self,$dsock,$xglob) = @_;
		if( (!$dsock->sock or !$dsock->peer_ip_string) && exists($self->{_SOCKETS}->{$xglob} ) ) {
			$self->warn("<$xglob> is not connected yet, killing it : ".$dsock->sock) if NETDEBUG;
			my $sref      = $self->{_SOCKETS}->{$xglob} or $self->panic("<$xglob> is not registered!");
			my $handle_id = $sref->{handle}             or $self->panic("$xglob has no handle!");
			my $cbacks    = $self->{_HANDLES}->{$handle_id}->{cbacks};
			if(my $cbn = $cbacks->{Close}) { $handle_id->$cbn($xglob); }
			$self->RemoveSocket($handle_id,$xglob);
		}
	}
	
	sub _TCP_Accept {
		my($self, $dsock) = @_;
		
		my $new_sock  = $dsock->sock->accept;
		my $new_ip    = 0;
		my $handle_id = $self->{_SOCKETS}->{$dsock->sock}->{handle} or $self->panic("No handle id?");
		my $hxref     = $self->{_HANDLES}->{$handle_id}             or $self->panic("No handle reference for $handle_id");
		my $cbacks    = $hxref->{cbacks};
		
		unless($new_sock) {
			$self->warn("accept() call failed?!");
		}
		elsif(! ($new_ip = $new_sock->peerhost) ) {
			$self->debug("No IP for $new_sock");
			$new_sock->close;
		}
		elsif($self->IpIsBlacklisted($handle_id, $new_ip)) {
			$self->debug("Refusing incoming connection from blacklisted ip $new_ip");
			$new_sock->close;
		}
		elsif($self->{avfds} < 1) {
			$self->warn("running out of filedescriptors, refusing incoming TCP connection");
			$new_sock->close;
		}
		elsif($hxref->{avpeers} < 1) {
			$self->warn("$handle_id : is full: won't accept new peers");
			$new_sock->close;
		}
		elsif(!$self->Unblock($new_sock)) {
			$self->panic("Failed to unblock $new_sock : $!");
		}
		else {
			my $new_dsock = Bitflu::Network::Danga->new(sock=>$new_sock, on_read_ready => sub { $self->_TCP_Read(shift); }) or $self->panic;
			$self->warn(">> ".$new_dsock->sock." -> ".$new_ip) if NETDEBUG;
			$self->{_SOCKETS}->{$new_dsock->sock} = { dsock => $new_dsock, peerip=>$new_ip, handle=>$handle_id, incoming=>1, lastio=>$self->GetTime, writeq=>'', qlen=>0, dtimer=>undef };
			$self->{avfds}--;
			$hxref->{avpeers}--;
			if(my $cbn = $cbacks->{Accept}) { $handle_id->$cbn($new_dsock->sock,$new_ip); }
		}
	}
	
	
	sub _TCP_Read {
		my($self, $dsock) = @_;
		
		my $rref      = $dsock->read(BF_BUFSIZ);
		my $sref      = $self->{_SOCKETS}->{$dsock->sock} or $self->panic("Sock not ".$dsock->sock." not registered?");
		my $handle_id = $sref->{handle}                   or $self->panic("No handle id?");
		my $cbacks    = $self->{_HANDLES}->{$handle_id}->{cbacks};
		
		if(!defined($rref)) {
			if(my $cbn = $cbacks->{Close}) { $handle_id->$cbn($dsock->sock); }
			$self->RemoveSocket($handle_id,$dsock->sock);
		}
		else {
			my $len = length($$rref);
			$sref->{lastio}             = $self->GetTime;
			$self->{stats}->{raw_recv} += $len;
			$self->debug("RECV $len from ".$dsock->sock) if NETDEBUG;
			if(my $cbn = $cbacks->{Data}) { $handle_id->$cbn($dsock->sock, $rref, $len); }
		}
	}
	
	sub _UDP_Read {
		my($self,$dsock) = @_;
		
		my $sock      = $dsock->sock               or $self->panic("No socket?");
		my $sref      = $self->{_SOCKETS}->{$sock} or $self->panic("$sock has no _SOCKETS entry!");
		my $handle_id = $sref->{handle}            or $self->panic("$sock has no handle in _SOCKETS!");
		my $cbacks    = $self->{_HANDLES}->{$handle_id}->{cbacks};
		my $new_ip    = '';
		my $buffer    = undef;
		
		$sock->recv($buffer,BF_BUFSIZ);
		
		if(!($new_ip = $sock->peerhost)) {
			# Weirdo..
			$self->debug("<$sock> had no peerhost, data dropped");
		}
		elsif($self->IpIsBlacklisted($handle_id,$new_ip)) {
			$self->debug("Dropping UDP-Data from blacklisted IP $new_ip");
		}
		elsif(my $cbn = $cbacks->{Data}) {
				$handle_id->$cbn($sock, \$buffer);
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
			elsif($self->{bpc} > BF_BUFSIZ) { $self->{bpc} = BF_BUFSIZ }
		}
		
		$self->{stats}->{nextrun} = NETSTATS + $self->GetTime;
	}
	
	
	sub BlacklistIp {
		my($self, $handle_id, $this_ip, $ttl) = @_;
		
		my $xbl = $self->{_HANDLES}->{$handle_id}->{blacklist} or $self->panic("$handle_id was not registered!");
		   $ttl = BLIST_TTL unless $ttl;
		
		if($self->IsNativeIPv6($this_ip)) {
			$this_ip = $self->ExpandIpV6($this_ip);
		}
		
		unless($self->IpIsBlacklisted($handle_id, $this_ip)) {
			my $pointer = ( $xbl->{pointer} >= BLIST_LIMIT ? 0 : $xbl->{pointer});
			# Ditch old entry
			my $oldkey = $xbl->{array}->[$pointer];
			defined($oldkey) and delete($xbl->{bldb}->{$oldkey});
			$xbl->{array}->[$pointer] = $this_ip;
			$xbl->{bldb}->{$this_ip}  = $self->GetTime + $ttl;
			$xbl->{pointer}           = 1+$pointer;
		}
	}
	
	sub IpIsBlacklisted {
		my($self, $handle_id, $this_ip) = @_;
		
		my $xbl = $self->{_HANDLES}->{$handle_id}->{blacklist} or $self->panic("$handle_id was not registered!");
		
		if($self->IsNativeIPv6($this_ip)) {
			$this_ip = $self->ExpandIpV6($this_ip);
		}
		
		if(exists($xbl->{bldb}->{$this_ip}) && $self->GetTime < $xbl->{bldb}->{$this_ip}) {
			return 1;
		}
		else {
			return 0;
		}
	}
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug("Network : ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("Network : ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("Network : ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("Network : ".$msg); }
	sub stop  { my($self, $msg) = @_; $self->{super}->stop("Network : ".$msg); }
1;

###############################################################################################################
# Danga-Socket event dispatcher
package Bitflu::Network::Danga;
	use strict;
	use base qw(Danga::Socket);
	use fields qw(on_read_ready on_error on_hup);
	
	sub new {
		my($self,%args) = @_;
		$self = fields::new($self) unless ref $self;
		$self->SUPER::new($args{sock});
		
		foreach my $field qw(on_read_ready on_error on_hup) {
			$self->{$field} = $args{$field} if $args{$field};
		}
		
		$self->watch_read(1) if $args{on_read_ready}; # Watch out for read events
		return $self;
	}
	
	sub event_read {
		my($self) = @_;
		if(my $cx = $self->{on_read_ready}) {
			return $cx->($self);
		}
	}
	
	sub event_err {
		my($self) = @_;
		if(my $cx = $self->{on_error}) {
			return $cx->($self);
		}
	}
	
	sub event_hup {
		my($self) = @_;
		if(my $cx = $self->{on_hup}) {
			return $cx->($self);
		}
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
		
		unless(-f $self->{configuration_file}) {
			warn("-> Creating configuration file '$self->{configuration_file}'");
			open(CFGH, ">", $self->{configuration_file}) or die("Unable to create $self->{configuration_file}: $!\n");
			close(CFGH);
		}
		
		if (-f $self->{configuration_file}) {
			open(CFGH, "+<", $self->{configuration_file}) or die("Unable to open $self->{configuration_file} for writing: $!\n");
			$self->{configuration_fh} = *CFGH;
			
			# Try to create a backup
			if( open(BKUP, ">", $self->{configuration_file}.".backup") ) {
				while(<CFGH>) { print BKUP; }
				close(BKUP);
			}
		
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
				push(@A, [ ($self->IsRuntimeLocked($k) ? 3 : undef), sprintf("%-24s => %s",$k, $self->{conf}->{$k})]);
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
		
		# Remove obsoleted/ignored config settings
		foreach my $legacy_setting qw(torrent_minpeers readpriority writepriority sleeper tempdir) {
			delete($self->{conf}->{$legacy_setting});
		}
		
	}
	
	sub SetDefaults {
		my($self) = @_;
		$self->{conf}->{plugindir}       = './plugins';
		$self->{conf}->{pluginexclude}   = '';
		$self->{conf}->{workdir}         = "./workdir";
		$self->{conf}->{upspeed}         = 35;
		$self->{conf}->{loglevel}        = 5;
		$self->{conf}->{renice}          = 8;
		$self->{conf}->{logfile}         = '';
		$self->{conf}->{chdir}           = '';
		$self->{conf}->{history}         = 1;
		$self->{conf}->{ipv6}            = 1;
		$self->{conf}->{storage}         = 'StorageVFS';
		foreach my $opt qw(ipv6 renice plugindir pluginexclude workdir logfile storage chdir) {
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

################################################################################################
# Implements Simple-eXecution Tasks
package Bitflu::SxTask;

	sub new {
		my($classname,%args) = @_;
		my $sx = {
		           __super_=> $args{__SUPER_},
		           interval=> (exists($args{Interval}) ? $args{Interval} : 1),
		           super   => $args{Superclass},
		           cback   => $args{Callback},
		           args    => $args{Args},
		         };
		bless($sx,$classname);
		$sx->{_} = \$sx;
		return $sx;
	}
	
	sub run {
		my($self) = @_;
		my $cbx = $self->{cback};
		my $rv  = $self->{super}->$cbx(@{$self->{args}});
		$self->destroy if $rv == 0;
		return $self->{interval};
	}
	
	sub destroy {
		my($self) = @_;
		$self->{__super_}->DestroySxTask($self);
	}
	
1;


################################################################################################
# Bencoder lib
package Bitflu::Bencoder;
	
	sub decode {
		my($string) = @_;
		my $ref = { data=>$string, len=>length($string), pos=> 0 };
		Carp::confess("decode(undef) called") if $ref->{len} == 0;
		return undef if $string !~ /^[dli]/;
		return d2($ref);
	}
	
	sub encode {
		my($ref) = @_;
		Carp::confess("encode(undef) called") unless $ref;
		return _encode($ref);
	}
	
	
	
	sub _encode {
		my($ref) = @_;
		
		my $encoded = undef;
		
		Carp::cluck() unless defined $ref;
		
		if(ref($ref) eq "HASH") {
			$encoded .= "d";
			foreach(sort keys(%$ref)) {
				$encoded .= length($_).":".$_;
				$encoded .= _encode($ref->{$_});
			}
			$encoded .= "e";
		}
		elsif(ref($ref) eq "ARRAY") {
			$encoded .= "l";
			foreach(@$ref) {
				$encoded .= _encode($_);
			}
			$encoded .= "e";
		}
		elsif($ref =~ /^(\d+)$/) {
			$encoded .= "i$1e";
		}
		else {
			# -> String
			$encoded .= length($ref).":".$ref;
		}
		return $encoded;
	}
	

	sub d2 {
		my($ref) = @_;
		
		my $cc = _curchar($ref);
		if($cc eq 'd') {
			my $dict = {};
			for($ref->{pos}++;$ref->{pos} < $ref->{len};) {
				last if _curchar($ref) eq 'e';
				my $k = d2($ref);
				my $v = d2($ref);
				next unless defined $k; # whoops -> broken bencoding
				$dict->{$k} = $v;
			}
			$ref->{pos}++; # Skip the 'e'
			return $dict;
		}
		elsif($cc eq 'l') {
			my @list = ();
			for($ref->{pos}++;$ref->{pos} < $ref->{len};) {
				last if _curchar($ref) eq 'e';
				push(@list,d2($ref));
			}
			$ref->{pos}++; # Skip 'e'
			return \@list;
		}
		elsif($cc eq 'i') {
			my $integer = '';
			for($ref->{pos}++;$ref->{pos} < $ref->{len};$ref->{pos}++) {
				last if _curchar($ref) eq 'e';
				$integer .= _curchar($ref);
			}
			$ref->{pos}++; # Skip 'e'
			return $integer;
		}
		elsif($cc =~ /^\d$/) {
			my $s_len = '';
			while($ref->{pos} < $ref->{len}) {
				last if _curchar($ref) eq ':';
				$s_len .= _curchar($ref);
				$ref->{pos}++;
			}
			$ref->{pos}++; # Skip ':'
			
			return ''    if !$s_len;
			return undef if ($ref->{len}-$ref->{pos} < $s_len);
			my $str = substr($ref->{data}, $ref->{pos}, $s_len);
			$ref->{pos} += $s_len;
			return $str;
		}
		else {
			$ref->{pos} = $ref->{len};
			return undef;
		}
	}

	sub _curchar {
		my($ref) = @_;
		return(substr($ref->{data},$ref->{pos},1));
	}
1;
