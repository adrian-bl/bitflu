# A simple storage driver, using the local Filesystem as 'backend'
package Bitflu::StorageFarabDb;
use strict;

####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use constant COMMIT_CSIZE => 1024*512;

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
		$self->abort('=============================== READ ME ===============================');
	}
	
	$self->{super}->AddStorage($self);
	$self->{super}->Admin->RegisterCommand('commit', $self, '_Command_Commit'       , 'Start to assemble given hash', [[undef,'Usage: "commit queue_id [queue_id2 ...]"']]);
	$self->{super}->Admin->RegisterCommand('pcommit', $self,'_Command_Pcommit'      , 'Assemble selected files of given hash',
	    [[undef,'Usage: "pcommit queue_id [file-id1 file-id2...]"'], [undef, ''], [undef, 'Commit selected files, does a full commit if no files are specified'],
	     [1, 'Example: pcommit adecade0fb4df00ddeadb4bef00b4rb4df00ddea 5 8 10-15 (<-- ranges are also supported)'], [1, 'Use "file list adec.." to get the file-ids'] ]);
	
	$self->{super}->Admin->RegisterCommand('commits',$self, '_Command_Show_Commits' , 'Displays currently running commits');
	$self->{super}->Admin->RegisterCommand('files'  ,$self, '_Command_Files'        , 'Manages files of given queueid', [[undef,'Usage: "files list queue_id"']]);
	$self->{super}->AddRunner($self);
	return 1;
}


##########################################################################
# Storage mainloop (used while commiting/assembling files)
sub run {
	my($self) = @_;
	
	foreach my $sha (keys(%{$self->{assembling}})) {
		my $this_job          = $self->{assembling}->{$sha};
		my $this_entry        = $this_job->{E}->[$this_job->{I_E}];
		
		unless(defined($this_entry)) {
			delete($self->{assembling}->{$sha});
			my $commit_pfx  = '';
			my $commit_msg  = "$sha has been commited";
			my $commit_pic  = COMMIT_CLEAN;
			my $commit_fnum = int(@{$this_job->{E}});
			
			if($this_job->{C_E} != 0) {
				$commit_pfx  .= "INCOMPLETE_";
				$commit_pic  = COMMIT_BROKEN;
				$commit_msg .= ", $this_job->{C_E} pieces are still missing. Download incomplete";
			}
			
			if($this_job->{N_E} != $commit_fnum) {
				$commit_pfx .= "PARTIAL_";
				$commit_pic  = COMMIT_BROKEN;
				$commit_msg .= ", $commit_fnum of $this_job->{N_E} file(s) included";
			}
			
			rename($this_job->{P}, $self->_GetExclusiveDirectory($self->_GetXconf('completedir'), $commit_pfx.$this_job->{BN})) or $self->panic("Rename failed");
			
			if($this_job->{S}->GetSetting('committed') ne COMMIT_CLEAN) {
				$this_job->{S}->SetSetting('committed', $commit_pic );
			}
			
			$self->{super}->Admin->SendNotify($commit_msg);
			return undef;
		}
		
		my $bytes_done        = $this_job->{B_D};                   # Bytes done so far in this file
		my($path,$start,$end) = split(/\0/,$this_entry);
		my $file_size         = $end-$start;
		my $absolute_offset   = $start + $this_job->{B_D};
		my $xpiece            = int($absolute_offset/$this_job->{CS});
		my $xpiece_offset     = $absolute_offset - ($xpiece*$this_job->{CS});
		my $xpiece_overshoot  = ($xpiece+1 == $this_job->{CHUNKS} ? $this_job->{O} : 0);
		my $xpiece_xread      = $this_job->{CS}-$xpiece_offset-$xpiece_overshoot;
		
		$xpiece_xread = ($xpiece_xread > COMMIT_CSIZE ? COMMIT_CSIZE : $xpiece_xread);
		
		
		my $total_left        = $end-$absolute_offset;
		
		
		$xpiece_xread = $total_left if $total_left < $xpiece_xread;
		
		my (@a_path) = split("/",$path);
		my $d_file   = pop(@a_path);
		my $f_path   = $this_job->{P};
		my $xbuff    = undef;
		foreach my $xd (@a_path) {
			$f_path .= "/$xd";
			unless(-d $f_path) {
				mkdir($f_path) or $self->panic("Unable to create directory $f_path : $!");
			}
		}
		$f_path .= "/$d_file";
		
		$this_job->{B_D} += $xpiece_xread;
		$self->debug("R:$start=>$end FS:$file_size BD:$this_job->{B_D} AO:$absolute_offset CP:$xpiece POFF:$xpiece_offset OS:$xpiece_overshoot XR:$xpiece_xread P:$path");
		
		if($this_job->{S}->IsSetAsDone($xpiece)) {
			$xbuff = $this_job->{S}->ReadDoneData(Chunk=>$xpiece, Offset=>$xpiece_offset, Length=>$xpiece_xread);
		}
		else {
			$self->info("$xpiece\@$sha : Piece does not exist, simulating data ($xpiece_xread bytes)");
			$xbuff = "A" x $xpiece_xread;
			$this_job->{C_E}++ if $xpiece_xread != 0;
		}
		
		open(OFILE, ">>",$f_path) or $self->panic("Unable to write to $f_path : $!");
		print OFILE $xbuff or $self->panic("Error while writing to $f_path : $!");
		close(OFILE) or $self->panic;
		
		if($xpiece_xread == 0) {
			$this_job->{B_D} = 0;
			$this_job->{I_E}++;
		}
		
		return; # No MultiAssembling please
	}
	
}

sub _Command_Show_Commits {
	my($self) = @_;
	my @A = ();
	my $i = 0;
	foreach my $cstorage (keys(%{$self->{assembling}})) {
		my $cj = $self->{assembling}->{$cstorage};
		my $msg = sprintf("%s : Assembling file %d/%d. Megabytes written so far: %.3f",$cstorage, 1+$cj->{I_E}, int(@{$cj->{E}}), ($cj->{B_D}/1024/1024));
		push(@A,[undef, $msg]);
		$i++;
	}
	
	if($i == 0) {
		push(@A, [2, "No commits running"]);
	}
	
	return({CHAINSTOP=>1, MSG=>\@A});
}



sub _Command_Files {
	my($self, @args) = @_;
	
	my $command = $args[0];
	my $sha1    = $args[1];
	my $fid     = 0;
	my @A       = ();
	
	if($command eq 'list') {
		my $so = $self->OpenStorage($sha1);
		
		unless($so) {
			push(@A, [2, "Hash '$sha1' does not exist in queue"]);
		}
		else {
			my $flist = $so->GetSetting('filelayout');
			my $csize = $so->GetSetting('size') or $self->panic("$so : can't open 'size' object");
			push(@A,[3,sprintf("%s| %-64s %s %s", '#Id', 'Path', 'Size (MB)', 'Percent Done')]);
			foreach my $this_entry (split(/\n/,$flist)) {
				$fid++; # Increment file-id
				my($path,$start,$end) = split(/\0/,$this_entry);
				
				my $first_chunk = int($start/$csize);
				my $last_chunk  = int(($end/$csize) +0.5); # Ehr.. stimmt das so? bin gerade zu müde :-)
				my $num_chunks  = $last_chunk-$first_chunk+1; # Fixme. zu blöde zum rechnen
				my $done_chunks = 0;
				for(my $i=$first_chunk;$i<=$last_chunk;$i++) {
					$done_chunks++ if $so->IsSetAsDone($i);
				}
				
				# Gui-Crop-Down path
				$path = ((length($path) > FLIST_MAXLEN) ? substr($path,0,FLIST_MAXLEN-3)."..." : $path);
				my $msg = sprintf("%3d| %-64s %8.2f        %6.2f %%", $fid, $path, (($end-$start)/1024/1024), $done_chunks/$num_chunks*100);
				push(@A,[undef,$msg]);
			}
		}
	}
	else {
		push(@A,[2, "Invalid subcommand, try 'help files'"]);
	}
	return({CHAINSTOP=>1, MSG=>\@A});
}


##########################################################################
# Start partial or full commit
sub _Command_Pcommit {
	my($self, @args) = @_;
	
	my @A    = ();
	my $sha1 = shift(@args);
	my %f2c  = ();
	foreach (@args) {
		if($_ =~ /(\d+)-(\d+)/) { for($1..$2) { $f2c{$_} = 1; } }
		else                    { $f2c{$_} = 1; }
	}
	my $n_f2c= int(keys(%f2c));
	
	my $so = $self->OpenStorage($sha1);
	unless($so) {
		push(@A, [2, "'$sha1' does not exist. Committing non-existing files is not implemented (yet)"]);
	}
	elsif($so->CommitIsRunning) {
		push(@A, [2, "$sha1 : commit still running"]);
	}
	else {
			my $filelayout = $so->GetSetting('filelayout')               or $self->panic("$sha1: no filelayout found!");
			my $chunks     = $so->GetSetting('chunks')                   or $self->panic("$sha1: zero chunks?!");
			my $chunksize  = $so->GetSetting('size')                     or $self->panic("$sha1: zero sized chunks?!");
			my $overshoot  = $so->GetSetting('overshoot');                  $self->panic("$sha1: no overshoot defined?!") unless defined($overshoot);
			my $name       = $so->GetSetting('name')                     or $self->panic("$sha1: no name?!");
			my $tmpdir     = $self->_GetXconf('tempdir')                  or $self->panic("No tempdir?!");
			my $xname      = $self->_GetExclusiveDirectory($tmpdir,$name) or $self->panic("No exclusive name found for $name");
			my @entries    = ();
			my $is_pcommit = 0;
			my $numentry   = 0;
			
			if($n_f2c == 0) {
				# -> No extra args.. just take everything
				@entries = split(/\n/,$filelayout);
				$numentry = int(@entries);
			}
			else {
				my $this_e_i = 0;
				foreach my $this_e (split(/\n/,$filelayout)) {
					$numentry++;
					if($f2c{$numentry}) {
						push(@A, [1, "$sha1 : Partial commit: Including '".(split(/\0/,$numentry))[0]."'"]);
						push(@entries,$this_e);
					}
				}
			}
			
			if(int(@entries)) {
				mkdir($xname) or $self->panic("mkdir($xname) failed: $!");
				# -> This creates an assembling job
				$self->{assembling}->{$sha1} = { S=>$so, N_E => $numentry, E => \@entries, CS => $chunksize, CHUNKS => $chunks, #SObj, NumEntry, Entry, CS, Chunks
				                                 I_E => 0, C_E => 0, B_D => 0, O => $overshoot, P=>$xname, BN=>$name }; # IndexEntry CommitErrors BytesDone Overshoot Path BaseName
				push(@A, [3, "$sha1 : commit started"]);
			}
			else {
				push(@A, [2, "$sha1 : No files selected, cannot commit"]);
			}
	}
	
	return({CHAINSTOP=>1, MSG=>\@A});
}


##########################################################################
# Same as pcommit, but takes multiple Hashes as arguments and always does
# a full commit
sub _Command_Commit {
	my($self, @args) = @_;
	my @A = ();
	foreach my $cstorage (@args) {
		my $h = $self->_Command_Pcommit($cstorage);
		push(@A, @{$h->{MSG}});
	}
	return({CHAINSTOP=>1, MSG=>\@A});
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
		
		
		foreach my $chunk (0..($StorageChunks-1)) {
			$self->debug("CreateStorage($StorageId) : Writing empty chunk $chunk");
			open(FAKEFILE, ">", $xobject->_GetFreeDir()."/".int($chunk)) or $self->panic("Unable to write chunk $chunk to workdir : $!");
			close(FAKEFILE);
		}
		
		
		$xobject->SetSetting('name',      "Unnamed storage created by <$self>");
		$xobject->SetSetting('chunks',    $StorageChunks);
		$xobject->SetSetting('size',      $StorageSize);
		$xobject->SetSetting('overshoot', $StorageOvershoot);
		$xobject->SetSetting('committed', 0);
		$xobject->SetSetting('filelayout', $flb);
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
		warn "#$sid# is cached\n";
		return $self->{socache}->{$sid};
	}
	else {
		my $StorageId = $self->_FsSaveStorageId($sid);
		my $storeroot = $self->_GetXconf('incompletedir')."/$StorageId";
		if(-d $storeroot) {
			print "#$sid# is a new storage!\n";
			$self->{socache}->{$sid} = Bitflu::StorageFarabDb::XStorage->new(_super => $self, storage_id=>$StorageId, storage_root=>$storeroot);
			return $self->OpenStorage($sid);
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

sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
sub abort { my($self, $msg) = @_; $self->{super}->abort(ref($self).": ".$msg); }
sub warn { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg); }


1;

package Bitflu::StorageFarabDb::XStorage;
use strict;

use constant DONEDIR     => ".done";
use constant WORKDIR     => ".working";
use constant FREEDIR     => ".free";
use constant SETTINGSDIR => ".settings";

##########################################################################
# Creates a new Xobject (-> Storage driver for an item)
sub new {
	my($class, %args) = @_;
	my $self = { _super => $args{_super}, storage_id=>$args{storage_id}, storage_root=>$args{storage_root}, scache => {} };
	bless($self,$class);
	return $self;
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
	return ( (defined($self->{_super}->{assembling}->{$self->_GetStorageId}) ? 1 : 0 ) );
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
	$key = $self->_CleanString($key);
	my $setdir = $self->_GetSettingsDir();
	$self->{scache}->{$key} = $val;
	return $self->_WriteFile("$setdir/$key",$val);
}

##########################################################################
# Get substorage settings (.settings)
sub GetSetting {
	my($self,$key) = @_;
	
	$key     = $self->_CleanString($key);
	my $xval = undef;
	
	if(defined($xval = $self->{scache}->{$key})) {
		# -> Cache hit!
	}
	else {
		$xval = $self->_ReadFile($self->_GetSettingsDir()."/$key");
		$self->{scache}->{$key} = $xval;
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
	my $buff = undef;
	open(XFILE, "<", $file) or return undef;
	while(<XFILE>) { $buff .= $_; }
	close(XFILE);
	return $buff;
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
	my $chunk    = $args{Chunk};
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
	my $chunk    = $args{Chunk};
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




sub Truncate {
	my($self, $chunknum) = @_;
	my $workfile = $self->_GetWorkDir."/$chunknum";
	truncate($workfile, 0) or $self->panic("Unable to truncate chunk $workfile : $!");
}

sub SetAsInworkFromDone {
	my($self, $chunknum) = @_;
	my $source = $self->_GetDoneDir."/$chunknum";
	my $dest   = $self->_GetWorkDir."/$chunknum";
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	return undef;
}

sub SetAsInwork {
	my($self, $chunknum) = @_;
	my $source = $self->_GetFreeDir."/$chunknum";
	my $dest   = $self->_GetWorkDir."/$chunknum";
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	return undef;
}
sub SetAsDone {
	my($self, $chunknum) = @_;
	my $source = $self->_GetWorkDir."/$chunknum";
	my $dest   = $self->_GetDoneDir."/$chunknum";
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	return undef;
}

sub SetAsFree {
	my($self, $chunknum) = @_;
	my $source = $self->_GetWorkDir."/$chunknum";
	my $dest   = $self->_GetFreeDir."/$chunknum";
	rename($source,$dest) or $self->panic("rename($source,$dest) failed : $!");
	return undef;
}

sub IsSetAsFree {
	my($self, $chunknum) = @_;
	return (-e $self->_GetFreeDir."/$chunknum");
}

sub IsSetAsInwork {
	my($self, $chunknum) = @_;
	return (-e $self->_GetWorkDir."/$chunknum");
}
sub IsSetAsDone {
	my($self, $chunknum) = @_;
	return (-e $self->_GetDoneDir."/$chunknum");
}

sub GetSizeOfInworkPiece {
	my($self,$chunknum) = @_;
	my $statfile = $self->_GetWorkDir()."/".int($chunknum)."";
	my @STAT = stat($statfile);
	$self->panic("$chunknum does not exist in workdir!") if $STAT[1] == 0;
	return $STAT[7];
}

sub GetSizeOfFreePiece {
	my($self,$chunknum) = @_;
	my $statfile = $self->_GetFreeDir()."/".int($chunknum)."";
	my @STAT = stat($statfile);
	$self->panic("$chunknum does not exist in workdir!") if $STAT[1] == 0;
	return $STAT[7];
}
sub GetSizeOfDonePiece {
	my($self,$chunknum) = @_;
	my $statfile = $self->_GetDoneDir()."/".int($chunknum)."";
	my @STAT = stat($statfile);
	$self->panic("$chunknum does not exist in workdir!") if $STAT[1] == 0;
	return $STAT[7];
}


sub debug { my($self, $msg) = @_; $self->{_super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{_super}->info(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{_super}->panic(ref($self).": ".$msg); }


1;



__END__

