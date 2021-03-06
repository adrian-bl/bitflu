####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2011 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#
#

package Bitflu::StorageVFS;
use strict;
use POSIX;
use IO::Handle;
use Storable;

use constant _BITFLU_APIVERSION => 20120529;
use constant BITFLU_METADIR     => '.bitflu-meta-do-not-touch';
use constant SAVE_DELAY         => 180;
use constant ALLOC_BUFSIZE      => 4096;
use constant MAX_FHCACHE        => 8;           # Max number of cached filehandles

use constant CB_VFS_DIRTY       => 'vfs_dirty'; # Clipboard name for MultipleLaunch protection
use constant VFS_DIRTY_RUN      => 58;          # How often should the heartbeat sxtask run?

use constant VFS_FNAME_MAX      => 255;         # do not create filenames longer than 255 chars (maximum of most filesystems)
use constant VFS_PATH_MAX       => 1024;        # never-ever create a path longer than X chars

sub BEGIN {
	# Autoload Storable before going into chroot-jail
	Storable::thaw(Storable::nfreeze({}));
}

##########################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, conf => {}, so => {}, fhcache=>{}, cbcache=>{} };
	bless($self,$class);
	
	my $cproto = { incomplete_downloads => $mainclass->Configuration->GetValue('workdir')."/unfinished",
	               completed_downloads  => $mainclass->Configuration->GetValue('workdir')."/seeding",
	               unshared_downloads   => $mainclass->Configuration->GetValue('workdir')."/removed",
	               vfs_use_fallocate    => 0,
	               vfs_datafsync        => 1,
	             };
	
	foreach my $this_key (keys(%$cproto)) {
		my $this_value = $mainclass->Configuration->GetValue($this_key);
		if(!defined($this_value) or length($this_value) == 0) {
			$mainclass->Configuration->SetValue($this_key, $cproto->{$this_key});
		}
		$mainclass->Configuration->RuntimeLockValue($this_key);
	}
	
	# Shortcuts
	$self->{conf}->{dir_work}  = $mainclass->Configuration->GetValue('incomplete_downloads');
	$self->{conf}->{dir_done}  = $mainclass->Configuration->GetValue('completed_downloads');
	$self->{conf}->{dir_ushr}  = $mainclass->Configuration->GetValue('unshared_downloads');
	$self->{conf}->{dir_meta}  = $self->{conf}->{dir_work}."/".BITFLU_METADIR;
	
	my $mode = ( $mainclass->Configuration->GetValue('vfs_use_fallocate') ? "with fallocate (if supported)" : "sparsefiles" );
	
	$self->info("Using VFS storage plugin ($mode)");
	return $self;
}

##########################################################################
# Init 'workdir'
sub init {
	my($self) = @_;
	$self->{super}->AddStorage($self);
	
	foreach my $this_dname (qw(dir_work dir_done dir_ushr dir_meta)) {
		my $this_dir = $self->{conf}->{$this_dname};
		next if -d $this_dir;
		$self->debug("mkdir $this_dir");
		mkdir($this_dir) or $self->panic("Unable to create directory '$this_dir' : $!");
	}
	
	unless(-f $self->__GetClipboardFile) {
		$self->debug("Creating an empty clipboard");
		$self->__ClipboardStore;
	}
	
	$self->__ClipboardCacheInit;
	
	$self->{super}->AddRunner($self);
	$self->{super}->Admin->RegisterCommand('commit'  ,$self, '_Command_Commit' , 'Start to assemble given hash', [[undef,'Usage: "commit queue_id [queue_id2 ... | --all]"']]);
	$self->{super}->Admin->RegisterCommand('files'   ,$self, '_Command_Files'         , 'Manages files of given queueid', 
	                          [[0,'Usage: "files queue_id [list | commit fileId | exclude fileId | include fileId | priority fileId | clear fileId]"'], [0,''],
	                           [0,'files queue_id list            : List all files'],
	                           [0,'files queue_id list-included   : List only included files'],
	                           [0,'files queue_id list-excluded   : List only excluded files'],
	                           [0,'files queue_id exclude 1-3 8   : Do not download file 1,2,3 and 8'],
	                           [0,'files queue_id include 1-3 8   : Download file 1,2,3 and 8 (= remove "exclude" flag)'],
	                           [0,'files queue_id priority 1 5    : Raise Priority of file 1 and 5'],
	                           [0,'files queue_id clear 1 5       : Removes the "priority" flag from file 1 and 5'],
	                          ]);
	$self->{super}->Admin->RegisterCommand('rating'   ,$self, '_Command_Rating' , 'Display and modify rating',
	                          [[0,'Usage: rating queue_id [get | reset | set {value between 1-5}]'], [0,''],
	                           [0,'rating queue_id get            : Display local and remote rating'],
	                           [0,'rating queue_id reset          : Remove local/own rating'],
	                           [0,'rating queue_id set 3          : Rate "queue_id" 3 stars'],
	                          ]);
=head
# COMMENTSCODE
	$self->{super}->Admin->RegisterCommand('comment'  ,$self, '_Command_Comment' , 'Display and modify comments',
	                          [[0,'Usage: comment queue_id [get | set [@rating] text]'], [0,''],
	                           [0,'comment queue_id get           : Display comments of download'],
	                           [0,'comment queue_id set foobar    : Add \'foobar\' comment'],
	                           [0,'comment queue_id set @4 foobar : Add \'foobar\' comment and rank with 4 stars (ranking goes from 1-5)'],
	                          ]);
=cut
	
	$self->{super}->Admin->RegisterCommand('fhcache'  ,$self, '_CommandFhCache' , 'Display filehandle cache');
	
	$self->{super}->CreateSxTask(Superclass=>$self,Callback=>'_SxCheckCrashstate', Args=>[]);
	
	return 1;
}

##########################################################################
# Save metadata each X seconds
sub run {
	my($self) = @_;
	
	foreach my $sid (@{$self->GetStorageItems}) {
		$self->debug("Saving metadata of $sid");
		my $so = $self->OpenStorage($sid) or $self->panic("Unable to open $sid: $!");
		$so->_SaveMetadata;
	}
	
	return SAVE_DELAY;
}

##########################################################################
# Save Bitfields
sub terminate {
	my($self) = @_;
	# Simulate a metasave event:
	$self->info("saving metadata...");
	$self->run();
	$self->_FlushFileHandles;
	
	$self->info("removing storage-lock...");
	$self->ClipboardSet('vfs_dirty', 0); # no need to care about sxtasks: we are the last call before shutdown
}

##########################################################################
# Check if last shutdown was ok
# note: this can not be run directly from init() or run(): it should be
#       triggered via SxTask()
sub _SxCheckCrashstate {
	my($self) = @_;
	
	
	my $vfs_dirty = ( $self->ClipboardGet(CB_VFS_DIRTY) || 0 );
	my $vfs_skew  = ($vfs_dirty+VFS_DIRTY_RUN+5 - $self->{super}->Network->GetTime);
	
	if($vfs_skew > 0 && !$ENV{BITFLU_FORCE_START}) {
		$self->warn("bitflu is still running or has crashed recently. Try again in $vfs_skew seconds.");
		die;
	}
	elsif($vfs_dirty) {
		$self->warn("bitflu did not shutdown correctly: running background verify");
		my $queue = $self->{super}->Queue->GetQueueList;
		foreach my $proto (keys(%$queue)) {
			foreach my $sid (keys(%{$queue->{$proto}})) {
				$self->{super}->Admin->ExecuteCommand('verify', $sid);
			}
		}
	}
	
	# create 'heartbeat' process
	$self->{super}->CreateSxTask(Superclass=>$self,Callback=>'_SxMarkAlive', Interval=>VFS_DIRTY_RUN, Args=>[]);
	
	return 0;
}

##########################################################################
# mark process as 'alive'
sub _SxMarkAlive {
	my($self) = @_;
	$self->ClipboardSet(CB_VFS_DIRTY, $self->{super}->Network->GetTime);
	$self->debug("Setting alive flag");
	return 1;
}


##########################################################################
# Implements the 'commit' command
sub _Command_Commit {
	my($self, @args) = @_;
	my @A      = ();
	my $NOEXEC = '';
	$self->{super}->Tools->GetOpts(\@args);
	
	foreach my $sid (@args) {
		my $so = $self->OpenStorage($sid);
		if($so) {
			my $stats = $self->{super}->Queue->GetStats($sid);
			if($so->CommitFullyDone) {
				push(@A, [2, "$sid: has been committed"]);
			}
			elsif($stats->{total_chunks} == $stats->{done_chunks}) {
				my $newdir = $self->{super}->Tools->GetExclusiveDirectory($self->_GetDonedir, $self->_FsSaveDirent($so->GetSetting('name')));
				rename($so->_GetDataroot, $newdir) or $self->panic("Cannot rename ".$so->_GetDataroot." into $newdir: $!");
				$so->_SetDataroot($newdir);
				$so->SetSetting('committed',  1);
				push(@A, [1, "$sid: moved to $newdir"]);
			}
			else {
				push(@A, [2, "$sid: download not finished, refusing to commit"]);
			}
		}
		else {
			push(@A, [2, "$sid: does not exist in queue"]);
		}
	}
	
	unless(int(@args)) {
		$NOEXEC .= "Usage: commit queue_id [queue_id2 ...]";
	}
	
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}

##########################################################################
# Implements the 'files' command
sub _Command_Files {
	my($self, @args) = @_;
	
	my $sha1    = shift(@args) || '';
	my $command = shift(@args) || '';
	my $fid     = 0;
	my @A       = ();
	my $NOEXEC  = '';
	my $so      = undef;
	
	if(!($so = $self->OpenStorage($sha1))) {
		push(@A, [2, "Hash '$sha1' does not exist in queue"]);
	}
	elsif($command =~ /^list(-included|-excluded|)$/) {
		my $lopt  = $1;
		my $csize = $so->GetSetting('size') or $self->panic("$so : can't open 'size' object");
		my $pflag = $so->GetPriorityHash;
		push(@A,[3, [{vrow=>1, rsep=>'|'}, '#Id','Path', 'Size (MB)', '% Done']]);
		
		for(my $i=0; $i < $so->GetFileCount; $i++) {
			my $fp_info     = $so->GetFileProgress($i);
			my $this_file   = $so->GetFileInfo($i);
			my $done_chunks = $fp_info->{done};
			my $excl_chunks = $fp_info->{excluded};
			my $num_chunks  = $fp_info->{chunks};
			
			# Gui-Crop-Down path
			my $path   = $this_file->{path};
			my $pcdone = sprintf("%5.1f", ($done_chunks/$num_chunks*100) );
			my $prchar = ( $pflag->{$i} ? '^' : ' ' );
			if($pcdone >= 100 && $done_chunks != $num_chunks) {
				$pcdone = 99.99;
			}
			
			next if $excl_chunks  && $lopt eq '-included';
			next if !$excl_chunks && $lopt eq '-excluded';
			
			my $msg = [undef, sprintf("%3d%s",1+$i, $prchar), " $path ", sprintf("%8.2f",($this_file->{size}/1024/1024)), sprintf("%5.1f%%",$pcdone)];
			push(@A,[($excl_chunks == 0 ? 0 : 5 ),$msg]);
		}
		
	}
	elsif($command eq 'exclude' && defined $args[0]) {
		my $to_exclude  = $self->{super}->Tools->ExpandRange(@args);
		my $is_excluded = $so->GetExcludeHash;
		map { $is_excluded->{$_-1} = 1; } keys(%$to_exclude);
		$so->_SetExcludeHash($is_excluded);
		return $self->_Command_Files($sha1, "list");
	}
	elsif($command eq 'include' && defined $args[0]) {
		my $to_include  = $self->{super}->Tools->ExpandRange(@args);
		my $is_excluded = $so->GetExcludeHash;
		map { delete($is_excluded->{$_-1}) } keys(%$to_include);
		$so->_SetExcludeHash($is_excluded);
		return $self->_Command_Files($sha1, "list");
	}
	elsif($command eq 'priority' && defined $args[0]) {
		my $priorange  = $self->{super}->Tools->ExpandRange(@args);
		my $priohash   = $so->GetPriorityHash;
		map { $priohash->{$_-1} = 1; } keys(%$priorange);
		$so->_SetPriorityHash($priohash);
		return $self->_Command_Files($sha1, "list");	
	}
	elsif($command eq 'clear' && defined $args[0]) {
		my $to_clear = $self->{super}->Tools->ExpandRange(@args);
		my $priohash = $so->GetPriorityHash;
		map { delete($priohash->{$_-1}) } keys(%$to_clear);
		$so->_SetPriorityHash($priohash);
		return $self->_Command_Files($sha1, "list");	
	}
	else {
		$NOEXEC .= "Usage: files queue_id [list | list-included | list-excluded | exclude fileId | include fileId | priority fileId | clear fileId] type 'help files' for more information";
	}
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}


sub _Command_Rating {
	my($self, @args) = @_;
	
	my $sha1    = shift(@args) || '';
	my $command = shift(@args) || '';
	my ($value) = (shift(@args)|| '') =~ /(\d+)/;
	my @A       = ();
	my $NOEXEC  = '';
	my $so      = undef;
	
	if(!($so = $self->OpenStorage($sha1))) {
		push(@A, [2, "Hash '$sha1' does not exist in queue"]);
	}
	elsif($command eq 'get' or $command eq '') {
		my $own = ($so->GetLocalRating  || 'unset');
		my $rem = ($so->GetRemoteRating || 'unknown');
		push(@A, [undef, "own_rating=$own, remote=$rem"]);
	}
	elsif($command eq 'reset') {
		$so->SetLocalRating(0);
		push(@A, [undef, "rating of $sha1 has been reset"]);
	}
	elsif($command eq 'set' && $value && $value >= 1 && $value <= 5) {
		$so->SetLocalRating($value);
		push(@A, [undef, "rating of $sha1 set to $value"]);
	}
	else {
		$NOEXEC .= "Usage: rating queue_id [get | reset | set {value between 1-5}]";
	}
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}

=head
# COMMENTSCODE
##########################################################################
# Manages comments for given queue id
sub _Command_Comment {
	my($self, @args) = @_;
	my $sha1    = shift(@args) || '';
	my $command = shift(@args) || '';
	my $text    = join(" ",@args) || '';
	my @A       = ();
	my $NOEXEC  = '';
	my $so      = undef;
	
	if(!($so = $self->OpenStorage($sha1))) {
		push(@A, [2, "Hash '$sha1' does not exist in queue"]);
	}
	elsif($command eq 'get' or $command eq '') {
		my $comments = $so->GetComments;
		my $count    = 0;
		my $sorter   = {};
		foreach my $xref (@$comments) {
			$sorter->{$xref->{ts}.$count} = "rating=$xref->{rating}, at=".localtime($xref->{ts}).", comment=$xref->{text}";
			$count++;
		}
		if($count) {
			push(@A, [1, "$sha1: displaying $count comment(s)"]);
			map( { push(@A, [0, $sorter->{$_}]) } sort({$b<=>$a} keys(%$sorter)) );
		}
		else {
			push(@A, [2, "$sha1: no comments received (yet)"]);
		}
	}
	elsif($command eq 'set' && length($text)) {
		my $own_ranking = 0;
		if($text =~ /^@(\d)\s+(.+)$/) {
			$own_ranking = $1 if $1 >= 1 && $1 <= 5;
			$text        = $2;
		}
		$so->SetOwnComment($own_ranking, $text);
		push(@A, [1, "$sha1: comment '$text' saved."]);
	}
	else {
		$NOEXEC .= "Usage: comment queue_id [get | set [\@ranking] text]";
	}
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}
=cut

##########################################################################
# Create a new storage subdirectory
sub CreateStorage {
	my($self, %args) = @_;
	
	my $sid     = $args{StorageId};
	my $metadir = $self->_GetMetadir($sid);
	my $workdir = $self->{super}->Tools->GetExclusiveDirectory($self->_GetWorkdir, $self->_FsSaveDirent($sid));
	
	if($sid ne $self->_FsSaveStorageId($sid)) {
		$self->panic("$sid is not a valid storage id");
	}
	elsif(-d $metadir) {
		$self->panic("$metadir exists!");
	}
	elsif(!defined($workdir)) {
		$self->panic("Failed to find an exclusive directory for $sid");
	}
	else {
		mkdir($workdir) or $self->panic("Unable to mkdir($workdir) : $!"); # Create StoreRoot
		mkdir($metadir) or $self->panic("Unable to mkdir($metadir) : $!"); # Create metaroot
		
		# hack to write a setting before the storage is actually created
		Bitflu::StorageVFS::SubStore::_WriteFile(undef, "$metadir/dirty", 1);
		
		my $flo  = delete($args{FileLayout});
		my $flb  = '';
		my $ddup = {};
		foreach my $iref (@$flo) {
			my @a_path   = map($self->_FsSaveDirent($_), @{$iref->{path}}); # should be save now
			my $path     = join('/', @a_path );
			
			# check for too-long filenames or paths:
			if(length($path) > VFS_PATH_MAX) {
				my $new_path = "\@LongPath_".$self->{super}->Tools->sha1_hex($path);
				$self->warn("$sid path '$path' too long: converted into '$new_path'");
				$path = $new_path;
			}
			
			if(length($path) < 1 || $ddup->{$path}) {
				$self->warn("$sid invalid path: '$path'");
				for(my $i=0;; $i++) {
					$path = sprintf("invalid-path.%X",$i);
					last if !$ddup->{$path};
				}
			}
			
			$ddup->{$path} = 1;
			$flb          .= "$path\0$iref->{start}\0$iref->{end}\n";
		}
		
		if(int($args{Size}/$args{Chunks}) > 0xFFFFFFFF) {
			# FIXME: Better check ## XAU
			$self->stop("Chunksize too big, storage plugin can't handle such big values");
		}
		
		# unset it before we go into SubStore 'mode' (sets it again)
		Bitflu::StorageVFS::SubStore::_WriteFile(undef, "$metadir/dirty", 0);
		
		my $xobject = Bitflu::StorageVFS::SubStore->new(_super => $self, sid => $sid );
		
		# Prepare empty progress and done bitfields:
		for(1..$args{Chunks}) { $xobject->{bf}->{progress} .= pack("N",0); }
		$xobject->_InitBitfield($xobject->{bf}->{done}, ($args{Chunks}||0)-1);
		
		$xobject->SetSetting('name',       "Unnamed storage created by <$self>");
		$xobject->SetSetting('chunks',     $args{Chunks});
		$xobject->SetSetting('size',       $args{Size});
		$xobject->SetSetting('overshoot',  $args{Overshoot});
		$xobject->SetSetting('filelayout', $flb);
		$xobject->SetSetting('path',       $workdir);
		$xobject->SetSetting('committed',  0);
		$xobject->SetSetting('wipedata' ,  0); # remove data on ->Remove call (even if commited)
		$xobject->SetSetting('fallocate',  0); # true if ALL files were allocated with fallocate call
		$xobject->SetSetting('dirty',      0); # Unset dirty flag (set by SubStore->new)
		$xobject->_SaveMetadata;
		return $self->OpenStorage($sid);
	}
}

##########################################################################
# Open an existing storage
sub OpenStorage {
	my($self, $sid) = @_;
	
	if(exists($self->{so}->{$sid})) {
		return $self->{so}->{$sid};
	}
	if( ($sid eq $self->_FsSaveStorageId($sid)) && (-d $self->_GetMetadir($sid)) ) {
		$self->{so}->{$sid} = Bitflu::StorageVFS::SubStore->new(_super => $self, sid => $sid );
		my $so = $self->OpenStorage($sid);
		$so->_UpdateExcludeList;                      # Cannot be done in new() because it needs a complete storage
		$so->_SetDataroot($so->GetSetting('path'));   # Same here
		$so->_CreateDummyFiles;                       # Assemble dummy files
		$so->SetSetting('dirty',0);                   # Mark metadir as clean
		return $so;
	}
	else {
		return 0;
	}
}


##########################################################################
# Kill existing storage directory
sub RemoveStorage {
	my($self, $sid) = @_;
	
	my $basedir   = $self->{super}->Configuration->GetValue('workdir');
	my $metatmp   = $self->{super}->Tools->GetExclusiveTempdir($sid);
	my $ushrdir   = $self->_GetUnsharedDir;
	my $so        = $self->OpenStorage($sid) or $self->panic("Cannot remove non-existing storage with sid '$sid'");
	my $sname     = $self->_FsSaveDirent($so->GetSetting('name')); # FS-Save name entry
	my $committed = $so->CommitFullyDone;
	my $dataroot  = $so->_GetDataroot;
	my @slist     = $so->_ListSettings;
	   $sid       = $so->_GetSid or $self->panic;                  # Makes SID save to use
	
	# -> Now we ditch all metadata
	rename($self->_GetMetadir($sid), $metatmp)                                 or $self->panic("Cannot rename metadir to $metatmp: $!");
	foreach my $mkey (@slist) {
		unlink($metatmp."/".$mkey) or $self->panic("Cannot remove $mkey: $!");
	}
	rmdir($metatmp)             or $self->panic("rmdir($metatmp) failed: $!");  # Remove Tempdir
	delete($self->{so}->{$sid}) or $self->panic("Cannot remove socache entry"); # ..and cleanup the cache
	
	if($committed && !$so->GetSetting('wipedata')) {
		# Download committed (= finished) ? -> Move it to unshared-dir
		my $ushrdst = join("/",$ushrdir,$sname);
		
		# check if source and destination are not the same
		# they will be the same if completed_downloads == unshared_downloads and in this
		# case we will just skip the rename() call
		if( join(";",stat($ushrdst)) ne join(";",stat($dataroot)) ) {
			$ushrdst = $self->{super}->Tools->GetExclusiveDirectory($ushrdir, $sname) or $self->panic("Cannot get exclusive dirname");
			rename($dataroot, $ushrdst) or $self->panic("Cannot move $dataroot to $ushrdst: $!");
		}
		
		$self->{super}->Admin->SendNotify("$sid: Moved completed download into $ushrdst");
	}
	else {
		# Download was not finsihed (or wipe requested), we have to remove all data
		
		# We'll just re-use the $metatmp dir while working:
		rename($dataroot, $metatmp) or $self->panic("Could not move $dataroot to $metatmp: $!");
		
		for(my $i=0;$i<$so->GetFileCount;$i++) {
			my $finf      = $so->GetFileInfo($i);       # File info
			my $to_unlink = $metatmp."/".$finf->{path}; # Full path of file
			my @to_rmdir  = split('/',$finf->{path});   # Directory Arrow
			unlink($to_unlink);                         # Unlinking the file CAN fail (someone might have tampered with it)
			
			for(2..int(@to_rmdir)) { # 2 because we pop'em at the beginning
				pop(@to_rmdir);
				my $this_dirent = $metatmp."/".join('/',@to_rmdir);
				my $has_data    = -2;
				
				# Count number of dirents (if it still exists...)
				opendir(DIRENT, $this_dirent) or next;
				while(my $x = readdir(DIRENT)) { $has_data++; }
				close(DIRENT);
				
				unless($has_data) {
					# Empty? -> Remove it
					rmdir($this_dirent) or $self->panic("Cannot rmdir($this_dirent): $!");
				}
				
			}
		}
		rmdir($metatmp) or $self->warn("Could not remove $metatmp: directory not empty?");
		$self->info("$sid: Removed download from local filesystem");
	}
	
	$self->_FlushFileHandles;
	return 1;
}

sub ClipboardGet {
	my($self,$key) = @_;
	return $self->{cbcache}->{$key};
}

sub ClipboardSet {
	my($self,$key,$value) = @_;
	$self->{cbcache}->{$key} = $value;
	return $self->__ClipboardStore;
}

sub ClipboardRemove {
	my($self,$key) = @_;
	delete($self->{cbcache}->{$key}) or $self->panic("Cannot remove non-existing key '$key' from CB");
	return $self->__ClipboardStore;
}
sub ClipboardList {
	my($self) = @_;
	return(keys(%{$self->{cbcache}}));
}

sub __ClipboardCacheInit {
	my ($self) = @_;
	my ($cb, undef) = Bitflu::StorageVFS::SubStore::_ReadFile(undef,$self->__GetClipboardFile);
	$self->{cbcache} = Storable::thaw($cb);
	
	if(ref($self->{cbcache}) ne 'HASH') {
		$self->stop("ClipboardFile '".$self->__GetClipboardFile."' is corrupted! (try to remove the file)");
	}
}

sub __ClipboardStore {
	my($self) = @_;
	return Bitflu::StorageVFS::SubStore::_WriteFile(undef, $self->__GetClipboardFile, Storable::nfreeze($self->{cbcache}));
}

sub __GetClipboardFile {
	my($self) = @_;
	return $self->_GetMetabasedir."/clipboard";
}

##########################################################################
# Returns path to metas directory
sub _GetMetabasedir {
	my($self) = @_;
	return $self->{conf}->{dir_meta};
}

##########################################################################
# Returns direcotry of given sid
sub _GetMetadir {
	my($self,$sid) = @_;
	return $self->_GetMetabasedir."/".$self->_FsSaveStorageId($sid);
}

##########################################################################
# Return basedir of incomplete downloads
sub _GetWorkdir {
	my($self) = @_;
	return $self->{conf}->{dir_work};
}

##########################################################################
# Return basedir of completed downloads
sub _GetDonedir {
	my($self) = @_;
	return $self->{conf}->{dir_done};
}

##########################################################################
# Return basedir of unshared downloads
sub _GetUnsharedDir {
	my($self) = @_;
	return $self->{conf}->{dir_ushr};
}

##########################################################################
# Returns an array of all existing storage directories
sub GetStorageItems {
	my($self)     = @_;
	my $metaroot  = $self->_GetMetabasedir;
	my @Q         = ();
	opendir(XDIR, $metaroot) or return $self->panic("Unable to open $metaroot : $!");
	foreach my $item (readdir(XDIR)) {
		next if !(-d $metaroot."/".$item);
		next if $item eq ".";
		next if $item eq "..";
		push(@Q, $item);
	}
	closedir(XDIR);
	return \@Q;
}

##########################################################################
# Removes evil chars from StorageId
sub _FsSaveStorageId {
	my($self,$val) = @_;
	$val = lc($val);
	$val =~ tr/a-z0-9//cd;
	$val .= "0" x 40;
	return(substr($val,0,40));
}

##########################################################################
sub _FsSaveDirent {
	my($self, $val) = @_;
	$val =~ tr/\/\0\n\r/_/;
	$val =~ s/^\.\.?/_/;
	$val ||= "NULL";
	$val = "\@LongName_".$self->{super}->Tools->sha1_hex($val) if length($val) > VFS_FNAME_MAX;
	return $val;
}


##########################################################################
# Returns a filehandle and tries to cache it
sub _FetchFileHandle {
	my($self, $path) = @_;
	
	my $NOW  = $self->{super}->Network->GetTime;
	my $fhc  = $self->{fhcache};
	
	
	if(exists($fhc->{$path})) {
		$fhc->{$path}->{lastuse} = $NOW
	}
	else {
		
		# Fixme: This scales VERY bad.
		# Maybe we could stop the search after we hit something with age >= 20 or so..
		my @keys = keys(%$fhc);
		if(int(@keys) >= MAX_FHCACHE) {
			my $oldest_time = $NOW;
			my $oldest_path = undef;
			foreach my $key (@keys) {
				if($fhc->{$key}->{lastuse} <= $oldest_time) {
					$oldest_time = $fhc->{$key}->{lastuse};
					$oldest_path = $key;
				}
			}
			$self->_CloseFileHandle($oldest_path);
		}
		
		open(my($ofh), "+<", $path) or $self->panic("Cannot open file $path : $!");
		binmode($ofh);
		
		$fhc->{$path} = { fh=>$ofh, lastuse=>$NOW};
		
	}
	
	return $fhc->{$path}->{fh};
}

sub _FlushFileHandles {
	my($self) = @_;
	
	$self->debug("Flushing all cached filehandles:");
	
	foreach my $k (keys(%{$self->{fhcache}})) {
		$self->debug("_FlushFileHandles: $k");
		$self->_CloseFileHandle($k);
	}
}

sub _CloseFileHandle {
	my($self, $path) = @_;
	my $fhc  = $self->{fhcache};
	close($fhc->{$path}->{fh}) or $self->panic("Cannot close $path : $!");
	delete($fhc->{$path})      or $self->panic;
}

sub _CommandFhCache {
	my($self) = @_;
	
	my @MSG  = ();
	my $fhc  = $self->{fhcache};
	my $NOW  = $self->{super}->Network->GetTime;
	
	push(@MSG, [1, "Cached Filehandles:"]);
	foreach my $k (keys(%$fhc)) {
		push(@MSG, [3, sprintf(" > %-64s -> Unused since %d sec [$fhc->{$k}->{fh}]", substr($k,-64), ($NOW-($fhc->{$k}->{lastuse})))]);
	}
	
	return({MSG=>\@MSG, SCRAP=>[], NOEXEC=>''});
}

sub debug { my($self, $msg) = @_; $self->{super}->debug("Storage : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Storage : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Storage : ".$msg); }
sub stop { my($self, $msg) = @_; $self->{super}->stop("Storage : ".$msg); }
sub warn { my($self, $msg) = @_; $self->{super}->warn("Storage : ".$msg); }


1;

package Bitflu::StorageVFS::SubStore;
use strict;
use constant MAXCACHE     => 256;         # Do not cache data above 256 bytes
use constant CHUNKSIZE    => 1024*512;    # Must NOT be > than Network::MAXONWIRE;

use fields qw( _super sid scache bf fomap fo _datafsync );

sub new {
	my($class, %args) = @_;
	my $ssid = $args{_super}->_FsSaveStorageId($args{sid});
	
	my $ptype = {    _super => $args{_super},
	                   sid => $ssid,
	                scache => {},
	                    bf => { free => [], done => [], exclude => [], progress=>'' },
	                 fomap => [],
	                    fo => [],
	            _datafsync => 0,
	             };
	
	my $self = fields::new($class);
	map( $self->{$_} = delete($ptype->{$_}), keys(%$ptype) );
	
	$self->_KillCorruptedStorage($ssid) if $self->GetSetting('dirty'); # die if this was a dirty metadir
	$self->SetSetting('dirty', 1);                                     # still here? mark it as dirty
	
	# Set DataFsync flag
	$self->{_datafsync} = 1 if $self->{_super}->{super}->Configuration->GetValue('vfs_datafsync');
	
	# Cache file-layout
	my @fo      = split(/\n/, ( $self->GetSetting('filelayout') || '' ) ); # May not yet exist
	$self->{fo} = \@fo;
	
	# Build bitmasks:
	my $num_chunks = ($self->GetSetting('chunks') || 0)-1; # Can be -1
	my $c_size     = ($self->GetSetting('size'));
	
	# Init some internal stuff:
	$self->_InitBitfield($self->{bf}->{free}, $num_chunks);                        # everything is done
	$self->_SetBitfield($self->{bf}->{done}, $self->GetSetting('bf_done') || '');  # read from bitfield
	$self->{bf}->{progress} = $self->GetSetting('bf_progress');
	
	$self->_KillCorruptedStorage($ssid) if( $num_chunks+1 && $c_size < 1 );
	$self->_KillCorruptedStorage($ssid) if( $num_chunks+1 && ($num_chunks+1)*4 != length($self->{bf}->{progress}) );
	
	# Build freelist from done information
	for(0..$num_chunks) {
		if(! $self->_GetBit($self->{bf}->{done}, $_)) {
			$self->_SetBit($self->{bf}->{free}, $_);
		}
	}
	
	# We are almost done: build the fomap (index=first_piece, value=file_index)
	my $fo_i = 0;
	foreach my $this_fo (@fo) {
		my($fo_path, $fo_start, $fo_end) = split(/\0/, $this_fo); # fixme: this is only really needed for debugging - how about removing it?
		my ($piece_start, $piece_end)    = $self->GetPieceRange($fo_i);
		
		$self->debug("FoMap: Start=$piece_start, End=$piece_end, StreamStart=$fo_start, StreamEnd=$fo_end-1, Psize=$c_size, Index=$fo_i");
		
		for($piece_start..$piece_end) {
			push(@{$self->{fomap}->[$_]}, $fo_i);
		}
		$fo_i++;
	}
	
	
	return $self;
}

sub _KillCorruptedStorage {
	my($self,$ssid) = @_;
	
	$self->warn("$ssid: corrupted metadata detected!");
	
	my $cx_metadir  = $self->_GetMetadir($ssid);
	my $cx_dataroot = $self->_GetDataroot;
	my $cx_dumpdir  = $self->{_super}->{super}->Tools->GetExclusiveTempdir("$ssid.corrupted");
	
	$self->warn("$ssid: moving corrupted data into $cx_dumpdir");
	
	mkdir($cx_dumpdir) or $self->panic("failed to create directory $cx_dumpdir : $!");
	rename($cx_dataroot, "$cx_dumpdir/data") or $self->panic("failed to move $cx_dataroot : $!");
	rename($cx_metadir,  "$cx_dumpdir/meta") or $self->panic("failed to move $cx_metadir : $!");
	
	$self->stop("Stopping due to corrupted metadata. Please restart bitflu!");
	die "NOTREACHED";
}

sub _GetDataroot {
	my($self) = @_;
	return ( $self->GetSetting('path') or $self->panic("No dataroot?!") );
}

sub _SetDataroot {
	my($self, $path) = @_;
	$self->{_super}->_FlushFileHandles;
	$self->SetSetting('path', $path);
}

##########################################################################
# Return SID
sub _GetSid {
	my($self) = @_;
	return $self->{sid};
}

##########################################################################
# Return metadir
sub _GetMetadir {
	my($self) = @_;
	return ( $self->{_super}->_GetMetadir($self->{sid}) or $self->panic("No metadir?!") );
}

##########################################################################
# Store metadata for this SO
sub _SaveMetadata {
	my($self) = @_;
	$self->SetSetting('bf_done', $self->_DumpBitfield($self->{bf}->{done}));
	$self->SetSetting('bf_progress', $self->{bf}->{progress});
}

##########################################################################
# Returns '1' if given SID is currently commiting
sub CommitIsRunning {
	my($self) = @_;
	# Commit is never running
	return 0;
}

##########################################################################
# Returns '1' if this file has been assembled without any errors
sub CommitFullyDone {
	my($self) = @_;
	return $self->GetSetting('committed');
}

##########################################################################
# Returns '1' if this download uses sparsefiles
sub UsesSparsefile {
	my($self) = @_;
	return ( $self->GetSetting('fallocate') ? 0 : 1 );
}

##########################################################################
# Save a setting but only save it sometimes (for unimportant data)
sub SetSloppySetting {
	my($self,$key,$val) = @_;
	
	$self->panic("Invalid key: $key") if $key ne $self->_CleanString($key);
	my $oldval = $self->GetSetting($key);
	
	if(!defined($oldval) || int(rand(0xFF)) == 3) {
		return $self->SetSetting($key,$val);
	}
	# else: mem-update only
	$self->{scache}->{$key} = $val;
	return 1;
}

##########################################################################
# Save substorage settings (.settings)
sub SetSetting {
	my($self,$key,$val) = @_;
	
	$self->panic("Invalid key: $key") if $key ne $self->_CleanString($key);
	
	my $oldval = $self->GetSetting($key);
	
	if(defined($oldval) && $oldval eq $val) {
		return 1;
	}
	else {
		$self->{scache}->{$key} = $val;
		return $self->_WriteFile($self->_GetMetadir."/$key",$val); # update on-disk copy
	}
}

##########################################################################
# Get substorage settings (.settings)
sub GetSetting {
	my($self,$key) = @_;
	
	return $self->{scache}->{$key} if exists($self->{scache}->{$key});
	
	# -> cache miss
	$self->panic("Invalid key: $key") if $key ne $self->_CleanString($key);
	
	my ($xval,$size) = $self->_ReadFile($self->_GetMetadir."/$key");
	$self->{scache}->{$key} = $xval if $size <= MAXCACHE;
	
	return $xval;
}

##########################################################################
# Removes an item from .settings
sub _RemoveSetting {
	my($self,$key) = @_;
	
	$key  = $self->_CleanString($key);
	if(defined($self->GetSetting($key))) {
		unlink($self->_GetMetadir."/$key") or $self->panic("Unable to unlink $key: $!");
		delete($self->{scache}->{$key});
	}
	else {
		$self->panic("Cannot remove non-existing key '$key' from $self");
	}
}

##########################################################################
# Retuns a list of all settings
sub _ListSettings {
	my($self) = @_;
	my $mdir = $self->_GetMetadir;
	opendir(SDIR, $mdir) or $self->panic("Cannot open metadir of $self");
	my @list = grep { -f $mdir."/".$_ } readdir(SDIR);
	closedir(SDIR);
	return @list;
}


##########################################################################
# Bumps $value into $file
sub _WriteFile {
	my($self,$file,$value) = @_;
	Carp::cluck("OUCH! UNDEF WRITE\n") if !defined($value);
	
	my $tmpfile = $file.".\$tmp\$";
	open(XFILE, ">", $tmpfile) or $self->panic("Unable to write $tmpfile : $!");
	binmode(XFILE)             or $self->panic("Cannot set binmode in $tmpfile : $!");
	print XFILE $value         or $self->panic("Cannot write to $tmpfile : $!");
	
	if(!$self or $self->{_datafsync}) {
		# only call fsync during setup (!$self) or if it was
		# enabled in this reference
		XFILE->flush or $self->panic("Could not flush filehandle: $!");
		XFILE->sync  or $self->panic("Could not fsync filehandle: $!");
	}
	
	close(XFILE)               or $self->panic("Cannot close $tmpfile : $!");
	rename($tmpfile, $file)    or $self->panic("Unable to rename $tmpfile into $file : $!");
	return 1;
}

##########################################################################
# Reads WHOLE $file and returns string or undef on error
sub _ReadFile {
	my($self,$file) = @_;
	open(XFILE, "<", $file) or return (undef,0);
	binmode(XFILE)          or $self->panic("Cannot set binmode on $file : $!");
	my $size = (stat(*XFILE))[7];
	my $buff = join('', <XFILE>);
	close(XFILE);
	return ($buff,$size);
}

##########################################################################
# Removes evil chars
sub _CleanString {
	my($self,$string) = @_;
	$string =~ tr/0-9a-zA-Z\._ //cd;
	return $string;
}

##########################################################################
# Store given data inside a chunk, returns current offset, dies on error
sub WriteData {
	my($self, %args) = @_;
	my $offset       = $args{Offset};
	my $length       = $args{Length};
	my $dataref      = $args{Data};
	my $chunk        = int($args{Chunk});
	my $nogrow       = $args{NoGrow};
	my $foitems      = $self->{fomap}->[$chunk];
	my $strm_start   = $offset+($self->GetSetting('size')*$chunk);
	my $strm_end     = $strm_start+$length;
	my $chunk_border = ($self->GetSetting('size')*($chunk+1));
	my $didwrite     = 0;
	my $expct_offset= $offset+$length;
	
	$self->panic("Crossed pieceborder! ($strm_end > $chunk_border)") if $strm_end > $chunk_border;
	
	my $fox = {};
	foreach my $folink (@$foitems) {
		my $finf = $self->GetFileInfo($folink);
		push(@{$fox->{$finf->{start}}}, $finf);
	}
	
	foreach my $akey (sort({$a <=> $b} keys(%$fox))) {
		foreach my $finf (@{$fox->{$akey}}) {
			my $file_seek = 0;                                  # Seek to this position in file
			my $canwrite  = $length;                            # How much data we'll write
			my $fp        = $self->_GetDataroot."/$finf->{path}"; # Full Path
			
			if($strm_start > $finf->{start}) {
				# Requested data does not start at offset 0 -> Seek in file
				$file_seek = $strm_start - $finf->{start};
			}
			
			if($file_seek > $finf->{size}) {
				# File does not include this data
				next;
			}
			elsif($canwrite > ($finf->{size}-$file_seek)) {
				$canwrite = ($finf->{size}-$file_seek); # Cannot read so much data..
			}
			
			my $xfh = $self->{_super}->_FetchFileHandle($fp);
			sysseek($xfh, $file_seek, 0)                          or $self->panic("Cannot seek to position $file_seek in $fp : $!");
			(syswrite($xfh, ${$dataref}, $canwrite) == $canwrite) or $self->panic("Short write in $fp: $!");
			
			${$dataref} = substr(${$dataref}, $canwrite);
			$length -= $canwrite;
			$self->panic() if $length < 0;
		}
	}
	
	if($length) {
		$self->panic("Did not write requested data: length is set to $length (should be 0 bytes), sid:".$self->_GetSid);
	}
	
	substr($self->{bf}->{progress},$chunk*4,4,pack("N",$expct_offset)) unless $nogrow;
	
	return $expct_offset;
}

sub ReadDoneData {
	my($self, %args) = @_;
	$self->IsSetAsDone($args{Chunk}) or $self->panic("$args{Chunk} is NOT set as done!");
	return $self->_ReadData(%args);
}

sub ReadInworkData {
	my($self, %args) = @_;
	$self->IsSetAsInwork($args{Chunk}) or $self->panic("$args{Chunk} is NOT set as done!");
	return $self->_ReadData(%args);
}

sub _ReadData {
	my($self, %args) = @_;
	my $offset      = $args{Offset};
	my $length      = $args{Length};
	my $chunk       = int($args{Chunk});
	my $foitems     = $self->{fomap}->[$chunk];
	my $strm_start  = $offset+($self->GetSetting('size')*$chunk);
	my $strm_end    = $strm_start+$length;
	my $chunk_border = ($self->GetSetting('size')*($chunk+1));
	my $didread     = 0;
	my $buff        = '';
	
	$self->panic("Crossed pieceborder! ($strm_end > $chunk_border)") if $strm_end > $chunk_border;
	
	my $fox = {};
	foreach my $folink (@$foitems) {
		my $finf = $self->GetFileInfo($folink);
		push(@{$fox->{$finf->{start}}}, $finf);
	}
	
	foreach my $akey (sort({$a <=> $b} keys(%$fox))) {
		foreach my $finf (@{$fox->{$akey}}) {
			my $file_seek = 0;                                  # Seek to this position in file
			my $canread   = $length;                            # How much data we'll read
			my $fp        = $self->_GetDataroot."/$finf->{path}"; # Full Path
			my $xb        = '';                                 # Buffer for sysread output
			if($strm_start > $finf->{start}) {
				# Requested data does not start at offset 0 -> Seek in file
				$file_seek = $strm_start - $finf->{start};
			}
			
			if($file_seek > $finf->{size}) {
				# File does not include this data
				next;
			}
			elsif($canread > ($finf->{size}-$file_seek)) {
				$canread = ($finf->{size}-$file_seek); # Cannot read so much data..
			}
			
			my $xfh = $self->{_super}->_FetchFileHandle($fp);
			sysseek($xfh, $file_seek, 0)                                     or $self->panic("Cannot seek to position $file_seek in $fp : $!");
			(Bitflu::Tools::Sysread(undef,$xfh, \$xb, $canread) == $canread) or $self->panic("Short read in $fp (wanted $canread bytes at offset $file_seek): $!");
			
			$buff   .= $xb;
			$length -= $canread;
			$self->panic() if $length < 0;
		}
	}
	
	if($length) {
		$self->panic("Did not write requested data: length is set to $length (should be 0 bytes), sid:".$self->_GetSid);
	}
	
	
	return $buff;
}



####################################################################################################################################################
# Misc stuff
####################################################################################################################################################

sub Truncate {
	my($self, $chunknum, $newsize) = @_;
	$newsize = 0 unless $newsize;
	$self->IsSetAsInwork($chunknum) or $self->panic;
	substr($self->{bf}->{progress},$chunknum*4,4,pack("N",$newsize));
}


####################################################################################################################################################
# SetAs
####################################################################################################################################################

sub SetAsInworkFromDone {
	my($self, $chunknum) = @_;
	
	if($self->IsSetAsDone($chunknum)) {
		$self->_UnsetBit($self->{bf}->{done}, $chunknum);
	}
	else {
		$self->panic("Cannot call SetAsInworkFromDone($chunknum) on non-done piece");
	}
}

sub SetAsInwork {
	my($self, $chunknum) = @_;
	if($self->IsSetAsFree($chunknum)) {
		$self->_UnsetBit($self->{bf}->{free}, $chunknum);
	}
	else {
		$self->panic("Cannot call SetAsInwork($chunknum) on invalid piece");
	}
}

sub SetAsDone {
	my($self, $chunknum) = @_;
	if($self->IsSetAsInwork($chunknum)) {
		$self->_SetBit($self->{bf}->{done}, $chunknum);
	}
	else {
		$self->panic("Cannot call SetAsDone($chunknum) on invalid piece");
	}
}

sub SetAsFree {
	my($self, $chunknum) = @_;
	if($self->IsSetAsInwork($chunknum)) {
		$self->_SetBit($self->{bf}->{free}, $chunknum);
	}
	else {
		$self->panic("Cannot call SetAsFree($chunknum) on invalid piece");
	}
}

####################################################################################################################################################
# IsSetAs
####################################################################################################################################################
sub IsSetAsFree {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{free}, $chunknum);
}

sub IsSetAsInwork {
	my($self, $chunknum) = @_;
	if( !($self->_GetBit($self->{bf}->{free}, $chunknum)) && !($self->_GetBit($self->{bf}->{done}, $chunknum)) ) {
		return 1;
	}
	else {
		return 0;
	}
}

sub IsSetAsDone {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{done}, $chunknum);
}

sub IsSetAsExcluded {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{exclude}, $chunknum);
}


sub _SetBit {
	my $bitref = $_[1];
	my $bitnum = $_[2];
	my $bfIndex = int($bitnum / 8);
	$bitnum -= 8*$bfIndex;
	vec($bitref->[$bfIndex],(7-$bitnum),1) = 1;
}

sub _UnsetBit {
	my $bitref = $_[1];
	my $bitnum = $_[2];
	my $bfIndex = int($bitnum / 8);
	$bitnum -= 8*$bfIndex;
	vec($bitref->[$bfIndex],(7-$bitnum),1) = 0;
}

sub _GetBit {
	my $bitref = $_[1];
	my $bitnum = $_[2];
	die "Ouch\n" unless defined $bitnum;
	my $bfIndex = int($bitnum / 8);
	$bitnum -= 8*$bfIndex;
	return vec($bitref->[$bfIndex], (7-$bitnum), 1);
}

sub _InitBitfield {
	my $bitref = $_[1];
	my $count  = $_[2];
	my $bfLast = int($count / 8);
	for(0..$bfLast) {
		$bitref->[$_] = chr(0);
	}
}

sub _SetBitfield {
	my $bitref = $_[1];
	my $string = $_[2];
	for(my $i=0; $i<length($string);$i++) {
		$bitref->[$i] = substr($string,$i,1);
	}
}

sub _DumpBitfield {
	my($self, $bitref) = @_;
	return join('', @{$bitref});
}

####################################################################################################################################################
# Exclude and priority stuff
####################################################################################################################################################

## -> EXCLUDE
sub _UpdateExcludeList {
	my($self) = @_;
	
	my $num_chunks  = ($self->GetSetting('chunks')||0)-1;
	my $unq_exclude = $self->GetExcludeHash;
	my $ref_exclude = $self->{bf}->{exclude};
	
	# Pseudo-Exclude all pieces
	$self->_InitBitfield($ref_exclude,$num_chunks);
	for(0..$num_chunks) {
		$self->_SetBit($ref_exclude,$_);
	}
	
	# Now we are going to re-exclude all non-excluded files:
	for(my $i=0; $i < $self->GetFileCount; $i++) {
		unless($unq_exclude->{$i}) { # -> Not excluded -> Zero-Out all used bytes
			my ($first, $last) = $self->GetPieceRange($i);
			for($first..$last) { $self->_UnsetBit($ref_exclude,$_); }
		}
	}
}

sub GetExcludeHash {
	my($self) = @_;
	return $self->_GetGenericHashfile('exclude');
}

sub GetExcludeCount {
	my($self) = @_;
	my $eh = $self->GetExcludeHash;
	return int(keys(%$eh));
}

sub _SetExcludeHash {
	my($self, $ref) = @_;
	$self->SetSetting('exclude',join(',', keys(%$ref)));
	$self->_UpdateExcludeList;
}

## -> RATING

# Returns the 'local' rating, 0 if not set
sub GetLocalRating {
	my($self) = @_;
	return int( $self->GetSetting('rating_local') || 0 );
}

# updates the 'local' rating.. value==0 to unset it
sub SetLocalRating {
	my($self, $value) = @_;
	$self->SetSetting('rating_local', int($value));
}

# returns the avg. remote rating
sub GetRemoteRating {
	my($self) = @_;
	my $val    = ( $self->GetSetting('rating_remote') || "0 0" );
	my $rating = 0;
	
	if($val =~ /^([0-9\.]+) ([0-9\.]+)$/) {
		$rating = ($1 + $2) / 2;
	}
	
	return $rating;
}

# updates the remote rating.
sub UpdateRemoteRating {
	my($self,$value) = @_;
	my $old_rating = ($self->GetRemoteRating || $value || 0);
	$self->SetSetting('rating_remote', "$old_rating $value");
}

=head
## -> COMMENTSCODE

##########################################################################
# Updates the list of cached comments
sub UpdateComments {
	my($self, $carray) = @_;
	my $dedupe = {};
	my $limit  = 50; # never save more than 50 comments
	my $cached = $self->GetComments;
	foreach my $this_comment (@$carray, @$cached) {
		my $txt = unpack("H*", $this_comment->{text});
		next if length($txt) == 0;           # wont save empty comments
		next if exists($dedupe->{$txt});     # already exists
		next if $this_comment->{rating} < 0; # rating is always >= 0
		next if $this_comment->{rating} > 5; # 5 stars are the max
		$dedupe->{$txt} = join(" ", $txt, abs(int($this_comment->{rating})), abs(int($this_comment->{ts})));
		last if --$limit < 0;
	}
	my @alist = map({ $_ } values(%$dedupe));
	$self->SetSloppySetting('comments_cache', join("\n", @alist));
}

##########################################################################
# Returns all cached comments (including our own comment)
sub GetComments {
	my($self) = @_;
	my @l    = ();                   # returned list
	my $own  = $self->GetOwnComment; # reference to own comment, may be undef
	my $skip = '';                   # skip comments with this text
	
	if($own) {
		push(@l, $own);       # add own comment
		$skip = $own->{text}; # and ditch our own from the cached copy
	}
	
	foreach my $item (split("\n", ($self->GetSetting('comments_cache') || ''))) {
		my @parts = split(" ", $item);
		my $text  = pack("H*", $parts[0]);
		push(@l, { text=>$text, rating=>$parts[1], ts=>$parts[2] }) if $text ne $skip;
	}
	
	return \@l;
}

##########################################################################
# Updates user-set comment
sub SetOwnComment {
	my($self, $rating, $text) = @_;
	$self->panic("invalid rating!") if $rating < 0 or $rating > 5;
	$self->SetSetting('comments_local', join(" ", unpack("H*",$text), abs(int($rating)), time()));
}

##########################################################################
# Returns user-set comment, undef if there is none
sub GetOwnComment {
	my($self) = @_;
	my $str = $self->GetSetting('comments_local') || '';
	return undef if length($str) == 0;
	my @parts = split(" ",$str);
	return { text=>pack("H*", $parts[0]), rating=>$parts[1], ts=>$parts[2] };
}
=cut

## -> PRIORITY
sub GetPriorityHash {
	my($self) = @_;
	return $self->_GetGenericHashfile('priority');
}

sub _SetPriorityHash {
	my($self,$ref) = @_;
	$self->SetSetting('priority', join(',',keys(%$ref)));
}

sub _GetGenericHashfile {
	my($self,$name) = @_;
	my $x   = {};
	my $fc  = $self->GetFileCount;
	my $str = $self->GetSetting($name);
	   $str = '' unless defined($str); # cannot use || because this would match '0'
	foreach my $item (split(/,/,$str)) {
		next if $item < 0 or $item >= $fc;
		$x->{$item}=1;
	}
	return $x;
}




sub GetSizeOfInworkPiece {
	my($self,$chunknum) = @_;
	$self->IsSetAsInwork($chunknum) or $self->panic;
	return unpack("N", substr($self->{bf}->{progress},$chunknum*4,4));
}
sub GetSizeOfFreePiece {
	my($self,$chunknum) = @_;
	$self->IsSetAsFree($chunknum) or $self->panic;
	return unpack("N", substr($self->{bf}->{progress},$chunknum*4,4));
}
sub GetSizeOfDonePiece {
	my($self,$chunknum) = @_;
	$self->IsSetAsDone($chunknum) or $self->panic;
	return unpack("N", substr($self->{bf}->{progress},$chunknum*4,4));
}


##########################################################################
# Gets a single file chunk
sub GetFileChunk {
	my($self,$file,$chunk) = @_;
	
	$file  = int($file);
	$chunk = int($chunk);
	my $fi      = $self->GetFileInfo($file);            # FileInfo reference
	my $fp      = $self->_GetDataroot."/".$fi->{path};  # Full Path
	my $psize   = $self->GetSetting('size');            # Piece Size
	my $offset  = CHUNKSIZE*$chunk;                     # File offset
	
	if($offset >= $fi->{size}) {
		return undef;
	}
	else {
		my $canread     = $fi->{size} - $offset;
		   $canread     = ($canread > CHUNKSIZE ? CHUNKSIZE : $canread);
		my $thisp_start = int($fi->{start}/$psize);
		my $thisp_end   = int(($fi->{start}+$canread)/$psize);
		my $xb          = '';
		my $xfh         = $self->{_super}->_FetchFileHandle($fp);
		sysseek($xfh, $offset, 0)                                         or $self->panic("Cannot seek to offset $offset in $fp: $!");
		(Bitflu::Tools::Sysread(undef, $xfh, \$xb, $canread) == $canread) or $self->panic("Failed to read $canread bytes from $fp: $!");
		
		return $xb;
	}
	$self->panic("Not reached");
}

##########################################################################
# Return FileLayout
sub __GetFileLayout {
	my($self) = @_;
	return $self->{fo};
}


##########################################################################
# 'Stat' a virtual file
sub GetFileInfo {
	my($self,$file) = @_;
	$file = int($file);
	my $x_entry = $self->__GetFileLayout->[$file] or $self->panic("No such file: $file");
	my ($path,$start,$end) = split(/\0/,$x_entry);
	return({path=>$path, start=>$start, end=>$end, size=>$end-$start});
}

##########################################################################
# Returns chunk information of given file id
sub GetFileProgress {
	my($self,$fid) = @_;
	
	my($p_first, $p_last) = $self->GetPieceRange($fid);
	my $done_chunks       = 0; # number of completed pieces/chunks
	my $excl_chunks       = 0; # nubmer of excluded pieces
	my $total_chunks      = 0; # total number of chunks used by this fid
	
	for(my $i=$p_first; $i<=$p_last; $i++) {
		$done_chunks++  if $self->IsSetAsDone($i);
		$excl_chunks++  if $self->IsSetAsExcluded($i);
		$total_chunks++;
	}
	
	return( { done=>$done_chunks, excluded=>$excl_chunks, chunks=>$total_chunks } );
}

##########################################################################
# Returns number of files
sub GetFileCount {
	my($self) = @_;
	my $fo = $self->__GetFileLayout;
	return int(@$fo);
}


##########################################################################
# Returns the first and last piece/chunk used by this file-index
sub GetPieceRange {
	my($self,$file) = @_;
	my $finfo = $self->GetFileInfo($file);
	my $csize = $self->GetSetting('size');
	my $chunks= $self->GetSetting('chunks');
	my $piece_start = int($finfo->{start}/$csize);
	my $piece_end   = $piece_start;
	
	if($finfo->{end} > $finfo->{start}) {
		$piece_end = int(( $finfo->{end}-1 )/$csize);
	}
	
	# ugly fixup for zero-sized piece at end
	$piece_start = $chunks-1 if $piece_start >= $chunks;
	$piece_end   = $chunks-1 if $piece_end   >= $chunks;
	
##	print ">> csize=$csize, start=$finfo->{start}, end=$finfo->{end}, start=$piece_start, end=$piece_end\n";
	
	return($piece_start,$piece_end);
}

##########################################################################
# Returns max size of given piece
sub GetTotalPieceSize {
	my($self, $piece) = @_;
	my $pieces = $self->GetSetting('chunks');
	my $size   = $self->GetSetting('size');
	if($pieces == (1+$piece)) {
		# -> LAST piece
		$size -= $self->GetSetting('overshoot');
	}
	return $size;
}

##########################################################################
# Creates/Fixes storage directory
sub _CreateDummyFiles {
	my($self) = @_;
	
	unless(-d $self->_GetDataroot) {
		$self->warn("Directory '".$self->_GetDataroot."' vanished! (queue-id: ".$self->_GetSid.")");
		$self->warn(" -> I'll try to recreate it (this download will start from zero again...)");
		$self->warn(" -> Please do *not* rename/delete or remove directories of active downloads!");
		mkdir($self->_GetDataroot) or $self->warn("mkdir() failed, going to panic soon.... : $!");
	}
	
	my $use_falloc = $self->{_super}->{super}->Configuration->GetValue('vfs_use_fallocate');
	my $statfs     = $self->{_super}->{super}->Syscall->statworkdir;
	my $dload_size = $self->GetSetting('size')*$self->GetSetting('chunks');
	my $falloc_ok  = 0;
	my $falloc_err = 0;
	
	
	if($use_falloc && ( !$statfs || $statfs->{bytes_free} <= $dload_size )) {
		$self->info($self->_GetSid.": disabling fallocate()");
		$use_falloc = 0;
	}
	
	for(my $i=0; $i<$self->GetFileCount;$i++) {
		my $finf   = $self->GetFileInfo($i);      # FileInfo
		my $d_path = $finf->{path};               # Path of this file
		my @a_path = split('/', $d_path);         # Array version
		my $d_file = pop(@a_path);                # Get filename
		my $d_base = $self->_GetDataroot;         # Dataroot prefix
		
		foreach my $dirent (@a_path) {
			$d_base .= "/".$dirent;
			next if -d $d_base;
			$self->debug("mkdir($d_base)");
			mkdir($d_base) or $self->panic("Failed to mkdir($d_base): $!");
		}
		
		my $filepath = $d_base."/".$d_file;
		
		if( !(-f $filepath) or ((-s $filepath) != $finf->{size}) ) {
			$self->debug("Creating/Fixing $filepath");
			open(XF, ">", $filepath)     or $self->panic("Failed to create sparsefile $filepath : $!");
			binmode(XF)                  or $self->panic("Cannot set binmode on $filepath : $!");
			sysseek(XF, $finf->{size},0) or $self->panic("Failed to seek to $finf->{size}: $!");
			
			if($use_falloc) {
				($self->{_super}->{super}->Syscall->fallocate(*XF, $finf->{size}) ? $falloc_err++ : $falloc_ok++);
			}
			
			truncate(XF, $finf->{size})  or $self->panic("Failed to truncate file to $finf->{size}: $!");
			close(XF)                    or $self->panic("Failed to close FH of $filepath : $!");
			
			my ($damage_start, $damage_end) = $self->GetPieceRange($i);
			
			for(my $d=$damage_start; $d <= $damage_end; $d++) {
				($self->IsSetAsDone($d) ? $self->SetAsInworkFromDone($d) : $self->SetAsInwork($d));
				$self->Truncate($d);  # Mark it as zero-size
				$self->SetAsFree($d); # ..and as free
			}
		}
	}
	
	if($falloc_err or $falloc_ok) {
		# tried to do some falloc: did it work?
		$self->warn("fallocate failed for ".$self->_GetSid." (disk full or unsupported filesystem?)") if $falloc_err;
		$self->SetSetting('fallocate', ($falloc_err ? 0 : 1 ) );
	}
	
}


sub debug { my($self, $msg) = @_; $self->{_super}->debug("XStorage: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{_super}->info("XStorage: ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{_super}->warn("XStorage: ".$msg);  }
sub stop  { my($self, $msg) = @_; $self->{_super}->stop("XStorage : ".$msg); }
sub panic { my($self, $msg) = @_; $self->{_super}->panic("XStorage: ".$msg); }


1;



__END__

