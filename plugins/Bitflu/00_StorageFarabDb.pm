# A simple storage driver, using the local Filesystem as 'backend'
package Bitflu::StorageFarabDb;
use strict;
use POSIX;
use IO::Handle;

####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#


use constant COMMIT_CLEAN  => '!';
use constant COMMIT_BROKEN => '¦';
use constant FLIST_MAXLEN  => 64;

##########################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, assembling => {}, socache => {} };
	bless($self,$class);
	return $self;
}

##########################################################################
# Init 'workdir'
sub init {
	my($self) = @_;
	
	$self->_SetXconf('workdir'      , $self->{super}->Configuration->GetValue('workdir'));
	$self->_SetXconf('incompletedir', $self->_GetXconf('workdir')."/".$self->{super}->Configuration->GetValue('incompletedir'));
	$self->_SetXconf('completedir'  , $self->_GetXconf('workdir')."/".$self->{super}->Configuration->GetValue('completedir'));
	$self->_SetXconf('tempdir'      , $self->_GetXconf('workdir')."/".$self->{super}->Configuration->GetValue('tempdir'));
	
	foreach my $cval (qw(workdir incompletedir completedir tempdir)) {
		$self->{super}->Configuration->RuntimeLockValue($cval);
		my $cdir = $self->_GetXconf($cval);
		$self->debug("$cval -> mkdir($cdir)");
		unless(-d $cdir) {
			mkdir($cdir) or $self->panic("Unable to create $cdir : $!");
		}
	}
	
	if(glob($self->_GetXconf('workdir')."/torrents/????????????????????????????????????????")) {
		# Detect legacy storage.. remove this after 2009 ;-)
		$self->warn("=============================== READ ME ===============================");
		$self->warn($self->_GetXconf('workdir')."/torrents/* exists!");
		$self->warn($self->_GetXconf('workdir')." appears to be a bitflu-0.3x storage directory.");
		$self->warn("Sadly it isn't compatible with this version of bitflu. You can either convert");
		$self->warn("the old data into the new format or run both versions at the same time");
		$self->warn("Checkout http://bitflu.workaround.ch/migrate.html for help!");
		$self->stop('=============================== READ ME ===============================');
	}
	
	$self->{super}->AddStorage($self);
	$self->{super}->Admin->RegisterCommand('commit', $self, '_Command_Commit'       , 'Start to assemble given hash', [[undef,'Usage: "commit queue_id [queue_id2 ...]"']]);
	$self->{super}->Admin->RegisterCommand('commits',$self, '_Command_Show_Commits' , 'Displays currently running commits');
	$self->{super}->Admin->RegisterCommand('files'  ,$self, '_Command_Files'        , 'Manages files of given queueid', 
	                          [[0,'Usage: "files queue_id [list | commit fileId | exclude fileId | include fileId]"'], [0,''],
	                           [0,'files queue_id list            : List all files'],
	                           [0,'files queue_id exclude 1-3 8   : Do not download file 1,2,3 and 8'],
	                           [0,'files queue_id include 1-3 8   : Download file 1,2,3 and 8 (= remove "exclude" flag)'],
	                           [0,'files queue_id commit 1-3 8    : Commit file 1,2,3 and 8'],
	                          ]);


	$self->{super}->AddRunner($self);
	return 1;
}


sub terminate {
	my($self) = @_;
	$self->info("Dumping storage layout");
	
	foreach my $sid (@{$self->GetStorageItems}) {
		$self->info("Dumping $sid");
		my $so = $self->OpenStorage($sid) or $self->panic("Unable to open $sid: $!");
		$so->ShutdownStorage;
	}
	
}


##########################################################################
# Storage mainloop (used while commiting/assembling files)
sub run {
	my($self) = @_;
	
	foreach my $sha (keys(%{$self->{assembling}})) {
		my $this_job     = $self->{assembling}->{$sha};
		my $this_eindex  = $this_job->{Entries}->[$this_job->{CurJob}];
		my $this_efile   = $this_job->{So}->RetrieveFileInfo($this_eindex);
		my (@a_path)     = split("/",$this_efile->{path});
		my $d_file       = pop(@a_path);
		my $f_path       = $this_job->{Path};
		
		
		foreach my $xd (@a_path) {
			$f_path .= "/".$self->_FsSaveDirent($xd);
			if($this_job->{CurWritten} == 0 && !(-d $f_path)) {
				# This is a new file (with a .. maybe.. new rootpath) without existing basedir
				# so just try to create it:
				mkdir($f_path) or $self->panic("Unable to create directory $f_path : $!");
			}
		}
		$f_path          .= "/$d_file";
		
		if($this_job->{CurWritten} == 0 && -e $f_path) {
			# Paranoia Check:
			$self->panic("File '$f_path' exists, but was not created by $0. This should not happen!");
		}
		
		my($x_buffer, $x_missed) = $this_job->{So}->RetrieveFileChunk($this_eindex, $this_job->{CurChunk}++);
		
		open(OUT, ">>", $f_path) or $self->panic("Unable to open $f_path : $!");
		if(defined($x_buffer)) {
			print OUT $x_buffer or $self->panic("Unable to write to $f_path : $!");
			$this_job->{CurWritten} = tell(OUT);
			$this_job->{Err}       += $x_missed;
		}
		else {
			$this_job->{CurJob}++;
			$this_job->{CurChunk}   = 0;
			$this_job->{CurWritten} = 0;
		}
		close(OUT);
		
		
		if($this_job->{CurJob} == $this_job->{NumEntry}) {
			delete($self->{assembling}->{$sha});
			
			my $commit_msg  = "$sha has been commited";
			my $commit_pic  = COMMIT_CLEAN;
			my $commit_fnum = $this_job->{NumEntry};
			my $commit_pfx  = '';
			if($this_job->{Err} != 0) {
				$commit_pfx  .= "INCOMPLETE_";
				$commit_pic  = COMMIT_BROKEN;
				$commit_msg .= ", $this_job->{Err} bytes are still missing. Download incomplete";
			}
			if($this_job->{CurJob} != $this_job->{EntryCount}) {
				$commit_pfx .= "PARTIAL_";
				$commit_pic  = COMMIT_BROKEN;
				$commit_msg .= ", $commit_fnum of $this_job->{EntryCount} file(s) included";
			}
			
			rename($this_job->{Path}, $self->_GetExclusiveDirectory($self->_GetXconf('completedir'), $commit_pfx.$this_job->{BaseName})) or $self->panic("Rename failed");
			$this_job->{So}->SetSetting('committed', $commit_pic) if ($this_job->{So}->GetSetting('committed') ne COMMIT_CLEAN);
			$self->{super}->Admin->SendNotify($commit_msg);
		}
		return; # No MultiAssembling please
	}
	
}


sub _Command_Show_Commits {
	my($self) = @_;
	my @A = ();
	my $i = 0;
	foreach my $cstorage (keys(%{$self->{assembling}})) {
		my $c_info = $self->OpenStorage($cstorage)->CommitIsRunning or $self->panic;
		my $msg = sprintf("%s: Committing file %d of %d. %.2f Megabytes written (total size: %.2f)", $cstorage, $c_info->{file}, $c_info->{total_files},
		                                                                                             ($c_info->{written}/1024/1024),($c_info->{total_size}/1024/1024));
		push(@A,[undef, $msg]);
		$i++;
	}
	
	if($i == 0) {
		push(@A, [2, "No commits running"]);
	}
	
	return({ MSG=>\@A, SCRAP=>[] });
}



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
	elsif($command eq 'list') {
		my $csize = $so->GetSetting('size') or $self->panic("$so : can't open 'size' object");
		push(@A,[3,sprintf("%s| %-64s | %s | %s", '#Id', 'Path', 'Size (MB)', 'Percent Done')]);
		
		for(my $i=0; $i < $so->RetrieveFileCount; $i++) {
			my $this_file    = $so->RetrieveFileInfo($i);
			my $first_chunk  = int($this_file->{start}/$csize);
			my $num_chunks   = POSIX::ceil(($this_file->{size})/$csize);
			my $done_chunks  = 0;
			my $excl_chunks  = 0;
			for(my $j=0;$j<$num_chunks;$j++) {
				$done_chunks++ if $so->IsSetAsDone($j+$first_chunk);
				$excl_chunks++ if $so->IsSetAsExcluded($j+$first_chunk);
			}
			
			
			# Gui-Crop-Down path
			my $path   = ((length($this_file->{path}) > FLIST_MAXLEN) ? substr($this_file->{path},0,FLIST_MAXLEN-3)."..." : $this_file->{path});
			my $pcdone = sprintf("%5.1f", ($num_chunks > 0 ? ($done_chunks/$num_chunks*100) : 100));
			
			if($pcdone == 100 && $done_chunks != $num_chunks) {
				$pcdone -= 0.1;
			}
			
			my $msg = sprintf("%3d| %-64s | %8.2f  |     %5.1f%%", 1+$i, $path, (($this_file->{size})/1024/1024), $pcdone);
			push(@A,[($excl_chunks == 0 ? 0 : 5 ),$msg]);
		}
	}
	elsif($command eq 'exclude' && defined $args[0]) {
		my $to_exclude  = $self->{super}->Tools->ExpandRange(@args);
		my $is_excluded = $so->_GetExcludeHash;
		map { $is_excluded->{$_-1} = 1; } keys(%$to_exclude);
		$so->_SetExcludeHash($is_excluded);
		return $self->_Command_Files($sha1, "list");
	}
	elsif($command eq 'include' && defined $args[0]) {
		my $to_include  = $self->{super}->Tools->ExpandRange(@args);
		my $is_excluded = $so->_GetExcludeHash;
		map { delete($is_excluded->{$_-1}) } keys(%$to_include);
		$so->_SetExcludeHash($is_excluded);
		return $self->_Command_Files($sha1, "list");
	}
	elsif($command eq 'commit' && defined $args[0]) {
		return $self->_PieceCommit($sha1,@args);
	}
	else {
		$NOEXEC .= "Usage: files queue_id [list | commit fileId], type 'help files' for more information";
	}
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}


##########################################################################
# Start partial or full commit
sub _PieceCommit {
	my($self, @args) = @_;
	
	my @A      = ();
	my $sha1   = shift(@args);
	my $expand = $self->{super}->Tools->ExpandRange(@args);
	
	my $so = $self->OpenStorage($sha1);
	unless($so) {
		push(@A, [2, "queue item '$sha1' does not exist, cannot commit."]);
	}
	elsif($so->CommitIsRunning) {
		push(@A, [2, "$sha1 : commit still running"]);
	}
	else {
			my $chunks     = $so->GetSetting('chunks')                     or $self->panic("$sha1: zero chunks?!");
			my $name       = $self->_FsSaveDirent($so->GetSetting('name')) or $self->panic("$sha1: no name?!");
			my $tmpdir     = $self->_GetXconf('tempdir')                   or $self->panic("No tempdir?!");
			my $xname      = $self->_GetExclusiveDirectory($tmpdir,$name)  or $self->panic("No exclusive name found for $name");
			my $excludes   = $so->_GetExcludeHash;
			my $totalentry = $so->RetrieveFileCount;
			my @entries    = ();
			my $is_pcommit = 0;
			
			if(int(@args) == 0) {
				# -> No extra args.. just take everything that is not excluded
				for(0..($totalentry-1)) {
					push(@entries,$_) unless $excludes->{$_};
				}
			}
			else {
				for(my $i=0; $i<$totalentry; $i++) {
					if($expand->{$i+1}) {
						push(@A, [1, "$sha1 : Partial commit, including '".$so->RetrieveFileInfo($i)->{path}."'"]);
						push(@entries,$i);
					}
				}
			}
			
			if(int(@entries)) {
				mkdir($xname) or $self->panic("mkdir($xname) failed: $!");
				$self->{assembling}->{$sha1} = { So=>$so, EntryCount=>$totalentry, NumEntry=>int(@entries), Entries=>\@entries, Path=>$xname,
				                                 BaseName=>$name, CurJob=>0, CurChunk=>0, CurWritten=>0, Err=>0 };
				push(@A, [3, "$sha1 : commit started"]);
			}
			else {
				push(@A, [2, "$sha1 : No files selected, cannot commit"]);
			}
	}
	
	return({MSG=>\@A, SCRAP=>[] });
}


##########################################################################
# Same as pcommit, but takes multiple Hashes as arguments and always does
# a full commit
sub _Command_Commit {
	my($self, @args) = @_;
	my @A      = ();
	my $NOEXEC = '';
	foreach my $cstorage (@args) {
		my $h = $self->_PieceCommit($cstorage);
		push(@A, @{$h->{MSG}});
	}
	
	unless(int(@A)) {
		$NOEXEC .= "Usage: commit queue_id [queue_id2 ...]";
	}
	
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}


sub _GetExclusiveDirectory {
	my($self,$base,$id) = @_;
	
	my $xname = undef;
	foreach my $sfx (0..0xFFFF) {
		$xname = $base."/".$id;
		$xname .= ".$sfx" if $sfx != 0;
		unless(-d $xname) {
			return $xname;
		}
	}
	return $xname; # aka undef
}



##########################################################################
# Create a new storage subdirectory
sub CreateStorage {
	my($self, %args) = @_;
	
	my $StorageId         = $self->_FsSaveStorageId($args{StorageId});
	my $StorageSize       = $args{Size};
	my $StorageOvershoot  = $args{Overshoot};
	my $StorageChunks     = $args{Chunks};
	my $FileLayout        = $args{FileLayout};
	my $storeroot         = $self->_GetXconf('incompletedir')."/$StorageId";
	
	$self->debug("CreateStorage: $StorageId with size of $StorageSize and $StorageChunks chunks, overshoot: $StorageOvershoot \@ $storeroot");
	
	if($StorageChunks < 1) {
		$self->panic("FATAL: $StorageChunks < 1");
	}
	elsif(!-d $storeroot) {
		mkdir($storeroot) or $self->panic("Unable to create $storeroot : $!");
		
		my $xobject = Bitflu::StorageFarabDb::XStorage->new(_super => $self, storage_id=>$StorageId, storage_root=>$storeroot);
		
		my $flb = undef;
		foreach my $flk (keys(%$FileLayout)) {
			my $path = join('/',@{$FileLayout->{$flk}->{path}});
			$path =~ s/\n//gm; # Sorry.. no linebreaks here :-p
			$flb .= "$path\0$FileLayout->{$flk}->{start}\0$FileLayout->{$flk}->{end}\n";
		}
		
		mkdir($xobject->_GetDoneDir())     or $self->panic("Unable to create DoneDir : $!");	
		mkdir($xobject->_GetFreeDir())     or $self->panic("Unable to create FreeDir : $!");
		mkdir($xobject->_GetWorkDir())     or $self->panic("Unable to create WorkDir : $!");
		mkdir($xobject->_GetSettingsDir()) or $self->panic("Unable to create SettingsDir : $!");
		
		$xobject->_InitBitfield($xobject->{bf}->{Free}, ($StorageChunks-1)); # Fake fake fake :-)
		foreach my $chunk (0..($StorageChunks-1)) {
			$self->debug("CreateStorage($StorageId) : Writing empty chunk $chunk");
			$xobject->__CreateFreePiece($chunk);
		}
		
		
		$xobject->SetSetting('name',      "Unnamed storage created by <$self>");
		$xobject->SetSetting('chunks',    $StorageChunks);
		$xobject->SetSetting('size',      $StorageSize);
		$xobject->SetSetting('overshoot', $StorageOvershoot);
		$xobject->SetSetting('committed', 0);
		$xobject->SetSetting('filelayout', $flb);
		# $xobject is now a funky (unuseable) StorageObject: Filelayout and Bitmap caches are both wrong.
		# $xobject will be removed from memory after it went out-of-scope
		# What we return is a REAL StorageObject, provided by OpenStorage:
		return $self->OpenStorage($StorageId);
	}
	else {
		return undef;
	}
}

##########################################################################
# Open an existing storage
sub OpenStorage {
	my($self, $sid) = @_;
	
	if(defined($self->{socache}->{$sid})) {
		return $self->{socache}->{$sid};
	}
	else {
		my $StorageId = $self->_FsSaveStorageId($sid);
		my $storeroot = $self->_GetXconf('incompletedir')."/$StorageId";
		if(-d $storeroot) {
			$self->{socache}->{$sid} = Bitflu::StorageFarabDb::XStorage->new(_super => $self, storage_id=>$StorageId, storage_root=>$storeroot);
			my $so       = $self->OpenStorage($sid);
			my $chunks   = $so->GetSetting('chunks') or $self->panic("$sid has no chunks?!");
			
			# Destroy the bf-dump and update our own exclude list
			$so->SetSetting('bfdump', '');
			$so->_UpdateExcludeList;
			
			#3: Check all pieces
			for my $cc (1..$chunks) {
				$cc--; # Piececount starts at 0, but chunks at 1
				my $is_inwork = ($so->IsSetAsInwork($cc) ? 1 : 0);
				my $is_done   = ($so->IsSetAsDone($cc)   ? 1 : 0);
				my $is_free   = ($so->IsSetAsFree($cc)   ? 1 : 0);
				my $is_xsum   = $is_inwork+$is_done+$is_free;
				
				if($is_xsum == 0) {
					$self->warn("$StorageId is missing piece $cc, re-creating it");
					$so->__CreateFreePiece($cc);
				}
				elsif($is_xsum != 1) {
					$self->warn("$StorageId has multiple copies of piece $cc ($is_xsum), fixing...");
					if($is_inwork) { $so->__DitchInworkPiece($cc);                                }
					if($is_free)   { $so->SetAsInwork($cc); $so->__DitchInworkPiece($cc);         }
					if($is_done)   { $so->SetAsInworkFromDone($cc); $so->__DitchInworkPiece($cc); }
					$so->__CreateFreePiece($cc);
				}
				elsif($is_inwork) {
					$self->debug("$StorageId: Setting inwork-piece $cc as free");
					$so->SetAsFree($cc);
				}
				
			}
			
			return $so;
		}
		return 0;
	}
}

##########################################################################
# Kill existing storage directory
sub RemoveStorage {
	my($self, $sid) = @_;
	my $so           = $self->OpenStorage($sid) or $self->panic("Unable to open storage $sid");
	my $rootdir      = $so->_GetStorageRoot;
	my $destination  = $self->_GetExclusiveDirectory($self->_GetXconf('tempdir'), $so->_GetStorageId);
	
	delete($self->{assembling}->{$so->_GetStorageId}); # Remove commit jobs (if any)
	
	$self->debug("Shall remove this objects: $rootdir -> $destination");
	
	rename($rootdir,$destination) or $self->panic("Unable to rename '$rootdir' into '$destination' : $!");
	
	$so->__SetStorageRoot($destination);
	my $n_donedir = $so->_GetDoneDir;
	my $n_freedir = $so->_GetFreeDir;
	my $n_workdir = $so->_GetWorkDir;
	my $n_setdir  = $so->_GetSettingsDir;
	my $n_chunks  = $so->GetSetting('chunks');
	
	
	$self->debug("==> Removing chunks from download directores");
	foreach my $pxy (1..$n_chunks) {
		foreach my $pbase ($n_donedir, $n_freedir, $n_workdir) {
			my $fp = $pbase.'/'.($pxy-1);
			if(-f $fp) {
				$self->debug("Removing chunk $fp");
				unlink($fp) or $self->panic("Unable to remove '$fp' : $!");
				last; # Note: We do not check & panic if a chunk was not found: This will get detected while removing the dirs
			}
		}
	}
	
	$self->debug("==> Removing settings");
	opendir(SDIR, $n_setdir) or $self->panic("Unable to open $n_setdir : $!");
	while (my $setfile = readdir(SDIR)) {
		my $fp = $n_setdir.'/'.$setfile;
		next if -d $fp; # . and ..
		$self->debug("Removing $fp");
		unlink($fp) or $self->panic("Unable to remove settings-file '$fp' : $!");
	}
	closedir(SDIR) or $self->panic;
	
	$self->debug("==> Removing directories");
	foreach my $bfdir ($n_donedir, $n_freedir, $n_workdir, $n_setdir, $destination) {
		$self->debug("Unlinking directory $bfdir");
		rmdir($bfdir) or $self->panic("Unable to unlink $bfdir : $!");
	}
	
	delete($self->{socache}->{$sid}) or $self->panic("Unable to drop $sid from cache");
	return 1;
}

##########################################################################
sub _FsSaveDirent {
	my($self, $val) = @_;
	$val =~ tr/\/\0\n\r/_/;
	$val =~ s/^\.\.?/_/;
	$val ||= "NULL";
	return $val;
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
# Returns an array of all existing storage directories
sub GetStorageItems {
	my($self) = @_;
	my $storeroot = $self->_GetXconf('incompletedir');
	my @Q = ();
	opendir(XDIR, $storeroot) or return $self->panic("Unable to open $storeroot : $!");
	foreach my $item (readdir(XDIR)) {
		next if $item eq ".";
		next if $item eq "..";
		push(@Q, $item);
	}
	closedir(XDIR);
	return \@Q;
}

##########################################################################
# Set PRIVATE configuration options
sub _SetXconf {
	my($self,$key,$val) = @_;
	$self->panic("SetXconf($key,$val) => undef value!") if !defined($key) or !defined($val);
	
	if(!defined($self->{xconf}->{$key})) {
		$self->{xconf}->{$key} = $val;
	}
	else {
		$self->panic("$key is set!");
	}
	return 1;
}

##########################################################################
# Get PRIVATE configuration options
sub _GetXconf {
	my($self,$key) = @_;
	my $xval = $self->{xconf}->{$key};
	$self->panic("GetXconf($self,$key) : No value!") if !defined($xval);
	return $xval;
}

sub debug { my($self, $msg) = @_; $self->{super}->debug("Storage : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Storage : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Storage : ".$msg); }
sub stop { my($self, $msg) = @_; $self->{super}->stop("Storage : ".$msg); }
sub warn { my($self, $msg) = @_; $self->{super}->warn("Storage : ".$msg); }


1;

package Bitflu::StorageFarabDb::XStorage;
use strict;

use constant DONEDIR      => ".done";
use constant WORKDIR      => ".working";
use constant FREEDIR      => ".free";
use constant SETTINGSDIR  => ".settings";
use constant MAXCACHE     => 256;         # Do not cache data above 256 bytes
use constant COMMIT_CSIZE => 1024*512;   # Must NOT be > than Network::MAXONWIRE;

##########################################################################
# Creates a new Xobject (-> Storage driver for an item)
sub new {
	my($class, %args) = @_;
	my $self = { _super => $args{_super}, storage_id=>$args{storage_id}, storage_root=>$args{storage_root}, scache => {},
	             bf => { Free=>[], Done=>[], Work=>[], Exclude=>[] }, fo => [] };
	bless($self,$class);
	
	# Cache the FileLayout
	my @fo      = split(/\n/, ( $self->GetSetting('filelayout') || '' ) ); # May not yet exist
	$self->{fo} = \@fo;
	
	# ..and setup some storage stuff:
	my $chunks    = ($self->GetSetting('chunks')||0)-1; # FirstChunk = 0. This could be -1 if CreateStorage called us but 0..-1 does nothing anyway :-)
	my $bfdump    = ($self->GetSetting('bfdump')||'');  # Get (non existing?) bitfield dump
	my $statmsg   = '';
	my $totchunks = $chunks*3; # Free Done and Work
	my $tcc       = 0;
	my @xdirlist  = qw(Free Done Work);
	
	# Init all bitfields
	map($self->_InitBitfield($self->{bf}->{$_},$chunks), @xdirlist);
	# Calculate size of a single bitfield
	my $sbf_len  = length($self->_DumpBitfield($self->{bf}->{Free}));
	
	if(($sbf_len*int(@xdirlist)) == length($bfdump)) {
		# Bitfield cache matches contents, fine!
		for(my $i=0; $i<int(@xdirlist);$i++) {
			my $sbf_now = substr($bfdump, $i*$sbf_len, $sbf_len);
			$self->_SetBitfield($self->{bf}->{$xdirlist[$i]}, $sbf_now);
			$self->panic("Written bitfield is buggy?!") if $self->_DumpBitfield($self->{bf}->{$xdirlist[$i]}) ne $sbf_now;
		}
	}
	else {
		foreach my $stype (@xdirlist) {
			my $xsub = "_Get".$stype."Dir";
			my $sdir = $self->$xsub;
			for my $cc (0..$chunks) {
				$tcc++;
				if((-e $sdir."/".int($cc))) {
					$self->_SetBit($self->{bf}->{$stype},$cc);
				}
				else {
					$self->_UnsetBit($self->{bf}->{$stype},$cc);
				}
				if($tcc%23) { $statmsg = "Restoring broken bitfield, ".int($tcc/(1+$totchunks)*100)."% done [$stype] ... "; STDOUT->printflush("\r$statmsg"); }
			}
		}
		STDOUT->printflush("\r".(" " x length($statmsg))."\r");
	}
	
	return $self;
}


##########################################################################
# 'Invalidates' a storage-object
sub ShutdownStorage {
	my($self) = @_;
	
	# First we'll save a dump of our bitfield:
	my $bfbuff = '';
	foreach my $name qw(Free Done Work) {
		$bfbuff .= $self->_DumpBitfield($self->{bf}->{$name});
	}
	$self->SetSetting('bfdump', $bfbuff);
	
	# Now we'll invalidate this object
	bless($self, __PACKAGE__."::Vanished");
}


sub _GetDoneDir {
	my($self) = @_;
	return $self->{storage_root}."/".DONEDIR;
}
sub _GetFreeDir {
	my($self) = @_;
	return $self->{storage_root}."/".FREEDIR;
}
sub _GetWorkDir {
	my($self) = @_;
	return $self->{storage_root}."/".WORKDIR;
}
sub _GetSettingsDir {
	my($self) = @_;
	return $self->{storage_root}."/".SETTINGSDIR;
}
sub _GetStorageRoot {
	my($self) = @_;
	return $self->{storage_root};
}

sub _GetStorageId {
	my($self) = @_;
	return $self->{storage_id};
}

##########################################################################
# ForceSet a new root for this storage. You shouldn't use this
# unless you know what you are doing..
sub __SetStorageRoot {
	my($self,$new_root) = @_;
	$self->{storage_root} = $new_root;
}


##########################################################################
# Returns '1' if given SID is currently commiting
sub CommitIsRunning {
	my($self) = @_;
	
	if(my $cj = $self->{_super}->{assembling}->{$self->_GetStorageId}) {
		my $this_eindex = $cj->{Entries}->[$cj->{CurJob}];
		my $this_efile  = $cj->{So}->RetrieveFileInfo($this_eindex);
		return({ file=>1+$cj->{CurJob}, written=>$cj->{CurWritten}, total_files=>$cj->{NumEntry}, total_size=>$this_efile->{size} });
	}
	else {
		return 0;
	}
}

##########################################################################
# Returns '1' if this file has been assembled without any errors
sub CommitFullyDone {
	my($self) = @_;
	return( $self->GetSetting('committed') eq Bitflu::StorageFarabDb::COMMIT_CLEAN ? 1 : 0 )
}


##########################################################################
# Save substorage settings (.settings)
sub SetSetting {
	my($self,$key,$val) = @_;
	$key       = $self->_CleanString($key);
	my $oldval = $self->GetSetting($key);
	
	if(defined($oldval) && $oldval eq $val) {
		# -> No need to write data
		return 1;
	}
	else {
		my $setdir = $self->_GetSettingsDir();
		$self->{scache}->{$key} = $val;
		return $self->_WriteFile($self->_GetSettingsDir()."/$key",$val);
	}
}

##########################################################################
# Get substorage settings (.settings)
sub GetSetting {
	my($self,$key) = @_;
	
	$key     = $self->_CleanString($key);
	my $xval = undef;
	my $size = 0;
	if(exists($self->{scache}->{$key})) {
		$xval = $self->{scache}->{$key};
	}
	else {
		($xval,$size) = $self->_ReadFile($self->_GetSettingsDir()."/$key");
		$self->{scache}->{$key} = $xval if $size <= MAXCACHE;
	}
	return $xval;
}

##########################################################################
# Bumps $value into $file
sub _WriteFile {
	my($self,$file,$value) = @_;
	open(XFILE, ">", $file) or $self->panic("Unable to write $file : $!");
	print XFILE $value;
	close(XFILE);
	return 1;
}

##########################################################################
# Reads WHOLE $file and returns string or undef on error
sub _ReadFile {
	my($self,$file) = @_;
	open(XFILE, "<", $file) or return (undef,0);
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
	
	my $dataref  = $args{Data};
	my $offset   = $args{Offset};
	my $length   = $args{Length};
	my $chunk    = int($args{Chunk});
	my $s_size   = $self->GetSetting('size')   or $self->panic("No size?!");
	my $s_chunks = $self->GetSetting('chunks') or $self->panic("No chunks?!");
	my $workfile = $self->_GetWorkDir."/$chunk";
	my $bw       = 0;
	open(WF, "+<", $workfile)                                 or $self->panic("Unable to open $workfile : $!");
	sysseek(WF,$offset,0)                                     or $self->panic("Unable to seek to offset $offset : $!");
	$bw=syswrite(WF, ${$dataref}, $length); if($bw != $length) { $self->panic("Failed to write $length bytes (wrote $bw) : $!") }
	close(WF)                                                 or $self->panic("Unable to close filehandle : $!");
	return $offset+$length;
}

##########################################################################
# Read specified data from given chunk at given offset
sub __ReadData {
	my($self, %args) = @_;
	my $offset   = $args{Offset};
	my $length   = $args{Length};
	my $chunk    = int($args{Chunk});
	my $xdir     = $args{XDIR};
	my $buff     = undef;
	my $workfile = $xdir."/$chunk";
	my $br       = 0;
	
	open(WF, "<", $workfile)                             or $self->panic("Unable to open $workfile : $!");
	sysseek(WF, $offset, 0)                              or $self->panic("Unable to seek to offset $offset : $!");
	$br = sysread(WF, $buff, $length); if($br != $length) { $self->panic("Failed to read $length bytes (read: $br) : $!"); }
	close(WF)                                            or $self->panic("Unable to close filehandle : $!");
	return $buff;
}


sub ReadDoneData {
	my($self, %args) = @_;
	$args{XDIR} = $self->_GetDoneDir;
	return $self->__ReadData(%args);
}

sub ReadInworkData {
	my($self, %args) = @_;
	$args{XDIR} = $self->_GetWorkDir;
	return $self->__ReadData(%args);
}



####################################################################################################################################################
# Misc stuff
####################################################################################################################################################

sub Truncate {
	my($self, $chunknum) = @_;
	my $workfile = $self->_GetWorkDir."/".int($chunknum);
	truncate($workfile, 0) or $self->panic("Unable to truncate chunk $workfile : $!");
}

sub __CreateFreePiece {
	my($self, $chunknum) = @_;
	my $workfile = $self->_GetFreeDir."/".int($chunknum);
	open(FAKE, ">", $workfile) or $self->panic("Unable to create $workfile: $!");
	close(FAKE);
	$self->_SetBit($self->{bf}->{Free},$chunknum);
}

sub __DitchInworkPiece {
	my($self, $chunknum) = @_;
	my $workfile = $self->_GetWorkDir."/".int($chunknum);
	unlink($workfile) or $self->panic("Unable to truncate chunk $workfile : $!");
	$self->_UnsetBit($self->{bf}->{Work},$chunknum);
}

sub __GetFileLayout {
	my($self) = @_;
	return $self->{fo};
}
####################################################################################################################################################
# SetAs
####################################################################################################################################################

sub SetAsInworkFromDone {
	my($self, $chunknum) = @_;
	my $source = $self->_GetDoneDir."/".int($chunknum);
	my $dest   = $self->_GetWorkDir."/".int($chunknum);
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	$self->_UnsetBit($self->{bf}->{Done},$chunknum);
	$self->_SetBit($self->{bf}->{Work},$chunknum);
	return undef;
}

sub SetAsInwork {
	my($self, $chunknum) = @_;
	my $source = $self->_GetFreeDir."/".int($chunknum);
	my $dest   = $self->_GetWorkDir."/".int($chunknum);
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	$self->_UnsetBit($self->{bf}->{Free},$chunknum);
	$self->_SetBit($self->{bf}->{Work},$chunknum);
	return undef;
}
sub SetAsDone {
	my($self, $chunknum) = @_;
	my $source = $self->_GetWorkDir."/".int($chunknum);
	my $dest   = $self->_GetDoneDir."/".int($chunknum);
	
	open(TOSYNC, "+<", $source) or $self->panic("Cannot open '$source' : $!");
	IO::Handle::sync(*TOSYNC);
	close(TOSYNC);
	
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	$self->_UnsetBit($self->{bf}->{Work},$chunknum);
	$self->_SetBit($self->{bf}->{Done},$chunknum);
	return undef;
}

sub SetAsFree {
	my($self, $chunknum) = @_;
	my $source = $self->_GetWorkDir."/".int($chunknum);
	my $dest   = $self->_GetFreeDir."/".int($chunknum);
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	$self->_UnsetBit($self->{bf}->{Work},$chunknum);
	$self->_SetBit($self->{bf}->{Free},$chunknum);
	return undef;
}

####################################################################################################################################################
# IsSetAs
####################################################################################################################################################
sub IsSetAsFree {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{Free}, $chunknum);
}

sub IsSetAsInwork {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{Work}, $chunknum);
}
sub IsSetAsDone {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{Done}, $chunknum);
}
sub IsSetAsExcluded {
	my($self, $chunknum) = @_;
	return $self->_GetBit($self->{bf}->{Exclude}, $chunknum);
}


sub _SetBit {
	my($self,$bitref,$bitnum) = @_;
	my $bfIndex = int($bitnum / 8);
	$bitnum -= 8*$bfIndex;
	vec($bitref->[$bfIndex],(7-$bitnum),1) = 1;
}
sub _UnsetBit {
	my($self,$bitref,$bitnum) = @_;
	my $bfIndex = int($bitnum / 8);
	$bitnum -= 8*$bfIndex;
	vec($bitref->[$bfIndex],(7-$bitnum),1) = 0;
}
sub _GetBit {
	my($self,$bitref,$bitnum) = @_;
	my $bfIndex = int($bitnum / 8);
	$bitnum -= 8*$bfIndex;
	return vec($bitref->[$bfIndex], (7-$bitnum), 1);
}

sub _InitBitfield {
	my($self, $bitref,$count) = @_;
	my $bfLast = int($count / 8);
	for(0..$bfLast) {
		$bitref->[$_] = chr(0);
	}
}

sub _SetBitfield {
	my($self,$bitref,$string) = @_;
	for(my $i=0; $i<length($string);$i++) {
		$bitref->[$i] = substr($string,$i,1);
	}
}

sub _DumpBitfield {
	my($self, $bitref) = @_;
	return join('', @{$bitref});
}

####################################################################################################################################################
# Exclude stuff
####################################################################################################################################################

sub _UpdateExcludeList {
	my($self) = @_;
	
	my $piecesize   = $self->GetSetting('size') or $self->panic("BUG! No size?!");
	my $num_chunks  = ($self->GetSetting('chunks')||0)-1;
	my $unq_exclude = $self->_GetExcludeHash;
	my $ref_exclude = $self->{bf}->{Exclude};
	
	# Pseudo-Exclude all pieces
	$self->_InitBitfield($ref_exclude,$num_chunks);
	for(0..$num_chunks) {
		$self->_SetBit($ref_exclude,$_);
	}
	
	# Now we are going to re-exclude all non-excluded files:
	for(my $i=0; $i < $self->RetrieveFileCount; $i++) {
		unless($unq_exclude->{$i}) { # -> Not excluded -> Zero-Out all used bytes
			my $finfo = $self->RetrieveFileInfo($i);
			my $first = int($finfo->{start}/$piecesize);
			my $last  = int($finfo->{end}/$piecesize);
			for($first..$last) { $self->_UnsetBit($ref_exclude,$_); }
		}
	}
}

sub _GetExcludeHash {
	my($self) = @_;
	my $estr = $self->GetSetting('exclude');
	   $estr = '' unless defined($estr); # cannot use || because this would match '0'
	my %ex = map ({ int($_) => 1; } split(/,/,$estr));
	return \%ex;
}

sub _SetExcludeHash {
	my($self, $ref) = @_;
	my $str = join(',', keys(%$ref));
	$self->SetSetting('exclude',$str);
	$self->_UpdateExcludeList;
}

####################################################################################################################################################
# GetSize
####################################################################################################################################################

sub GetSizeOfInworkPiece {
	my($self,$chunknum) = @_;
	my $statfile = $self->_GetWorkDir()."/".int($chunknum)."";
	my @STAT = stat($statfile);
	$self->panic("$statfile does not exist in workdir!") if $STAT[1] == 0;
	return $STAT[7];
}
sub GetSizeOfFreePiece {
	my($self,$chunknum) = @_;
	my $statfile = $self->_GetFreeDir()."/".int($chunknum)."";
	my @STAT = stat($statfile);
	$self->panic("$statfile does not exist in freedir!") if $STAT[1] == 0;
	return $STAT[7];
}
sub GetSizeOfDonePiece {
	my($self,$chunknum) = @_;
	my $statfile = $self->_GetDoneDir()."/".int($chunknum)."";
	my @STAT = stat($statfile);
	$self->panic("$statfile does not exist in donedir!") if $STAT[1] == 0;
	return $STAT[7];
}


##########################################################################
# Gets a single file chunk
sub RetrieveFileChunk {
	my($self,$file,$chunk) = @_;
	
	$file  = int($file);
	$chunk = int($chunk);
	
	my $cc                = $self->RetrieveFileInfo($file);                            # Fetch file information
	my $piece_size        = $self->GetSetting('size');                                 # Size of a single storage chunk
	my $absolute_offset   = $cc->{start} + ($chunk*COMMIT_CSIZE);                      # Real Start-Offset
	my $bytes_left        = $cc->{end}-$absolute_offset;                               # Left size of current file
	my $toread            = ($bytes_left < COMMIT_CSIZE ? $bytes_left : COMMIT_CSIZE); # How much data we might read
	my $xbuff             = '';                                                        # Data Buffer
	my $xsimulated        = 0;                                                         # How many bytes we did simulate
	
	$self->debug("File=>$file, Chunk=>$chunk FileSize=>$cc->{size}, Start=>$cc->{start}, End=>$cc->{end}, Offset=>$absolute_offset, Left=>$bytes_left, ToRead=>$toread");
	
	return (undef,undef) if $bytes_left < 1; # Invalid offset or empty file
	
	while($toread) {
		my $current_piece   = int($absolute_offset/$piece_size);
		my $current_offset  = $absolute_offset-$current_piece*$piece_size;
		my $current_pleft   = $piece_size-$current_offset;
		my $current_canread = ($toread < $current_pleft ? $toread : $current_pleft);
		my $current_didread = 0;
		my $current_buff    = '';
		
		if($self->IsSetAsDone($current_piece)) {
			$current_buff    = $self->ReadDoneData(Chunk=>$current_piece, Offset=>$current_offset, Length=>$current_canread);
			$current_didread = length($current_buff);
		}
		else {
			$self->warn("$current_piece does not exist, simulating $current_canread bytes");
			$current_buff    = chr(0) x $current_canread;
			$xsimulated     += $current_canread;
			$current_didread = $current_canread;
		}
		
		# Bugcheck
		$self->debug("Cpiece=>$current_piece, CPoffset=>$current_offset, CPleft=>$current_pleft CanRead=>$current_canread, DidRead=>$current_didread ToRead=>$toread");
		$self->panic("Read nothing!")          if $current_didread == 0;
		$self->panic("Miscalulated \$canread") if $current_canread < 1;
		
		$absolute_offset += $current_didread;
		$toread          -= $current_didread;
		$xbuff           .= $current_buff;
	}
	return ($xbuff,$xsimulated);
}


##########################################################################
# 'Stat' a virtual file
sub RetrieveFileInfo {
	my($self,$file) = @_;
	$file = int($file);
	my $x_entry = $self->__GetFileLayout->[$file] or $self->panic("No such file: $file");
	my ($path,$start,$end) = split(/\0/,$x_entry);
	return({path=>$path, start=>$start, end=>$end, size=>$end-$start});
}

##########################################################################
# Returns true if file exists
sub RetrieveFileCount {
	my($self,$file) = @_;
	my $fo = $self->__GetFileLayout;
	return int(@$fo);
}


sub debug { my($self, $msg) = @_; $self->{_super}->debug("XStorage: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{_super}->info("XStorage: ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{_super}->warn("XStorage: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{_super}->panic("XStorage: ".$msg); }


1;



__END__

