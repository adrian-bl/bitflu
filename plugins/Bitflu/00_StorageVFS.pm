####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2008 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#
#
# A simple storage driver, using the local Filesystem as 'backend'
package Bitflu::StorageVFS;
use strict;
use POSIX;
use IO::Handle;
use Storable;
use constant _BITFLU_APIVERSION => 20080529;
use constant BITFLU_METADIR     => '.bitflu-meta-do-not-touch';
use constant SAVE_DELAY         => 18;
use constant FLIST_MAXLEN       => 64;

sub BEGIN {
	# Autoload Storable before going into chroot-jail
	Storable::thaw(Storable::nfreeze({}));
}

##########################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, conf => {}, so => {}, nextsave => 0 };
	bless($self,$class);
	
	my $cproto = { incomplete_downloads => $mainclass->Configuration->GetValue('workdir')."/unfinished",
	               completed_downloads  => $mainclass->Configuration->GetValue('workdir')."/finished"
	             };
	
	foreach my $this_key (keys(%$cproto)) {
		my $this_value = $mainclass->Configuration->GetValue($this_key);
		if(!defined($this_value) or length($this_value) == 0) {
			$mainclass->Configuration->SetValue($this_key, $cproto->{$this_key});
		}
		$mainclass->Configuration->RuntimeLockValue($this_key);
	}
	
	# Shortcuts
	$self->{conf}->{dir_work} = $mainclass->Configuration->GetValue('incomplete_downloads');
	$self->{conf}->{dir_done}  = $mainclass->Configuration->GetValue('completed_downloads');
	$self->{conf}->{dir_meta}  = $self->{conf}->{dir_work}."/".BITFLU_METADIR;
	$self->info("Using VFS storage plugin (WARNING: THIS PLUGIN IS ALPAH-QUALITY-CODE)");
	return $self;
}

##########################################################################
# Init 'workdir'
sub init {
	my($self) = @_;
	$self->{super}->AddStorage($self);
	
	foreach my $this_dname qw(dir_work dir_done dir_meta) {
		my $this_dir = $self->{conf}->{$this_dname};
		next if -d $this_dir;
		$self->debug("mkdir $this_dir");
		mkdir($this_dir) or $self->panic("Unable to create directory '$this_dir' : $!");
	}
	
	unless(-f $self->__GetClipboardFile) {
		$self->debug("Creating an empty clipboard");
		$self->__ClipboardStore({});
	}
	
	$self->{super}->AddRunner($self);
	$self->{super}->Admin->RegisterCommand('commit'  ,$self, '_Command_Commit' , 'Start to assemble given hash', [[undef,'Usage: "commit queue_id [queue_id2 ...]"']]);
	$self->{super}->Admin->RegisterCommand('files'   ,$self, '_Command_Files'         , 'Manages files of given queueid', 
	                          [[0,'Usage: "files queue_id [list | commit fileId | exclude fileId | include fileId]"'], [0,''],
	                           [0,'files queue_id list            : List all files'],
	                           [0,'files queue_id exclude 1-3 8   : Do not download file 1,2,3 and 8'],
	                           [0,'files queue_id include 1-3 8   : Download file 1,2,3 and 8 (= remove "exclude" flag)'],
	                          ]);
	return 1;
}

##########################################################################
# Save metadata each X seconds
sub run {
	my($self) = @_;
	my $NOW = $self->{super}->Network->GetTime;
	
	return if $NOW < $self->{nextsave};
	$self->{nextsave} = $NOW + SAVE_DELAY;
	foreach my $sid (@{$self->GetStorageItems}) {
		$self->debug("Saving metadata of $sid");
		my $so = $self->OpenStorage($sid) or $self->panic("Unable to open $sid: $!");
		$so->_SaveMetadata;
	}
}

##########################################################################
# Save Bitfields
sub terminate {
	my($self) = @_;
	# Simulate a metasave event:
	$self->{nextsave} = 0;
	$self->run;
}

##########################################################################
# Implements the 'commit' command
sub _Command_Commit {
	my($self, @args) = @_;
	my @A      = ();
	my $NOEXEC = '';
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
	elsif($command eq 'list') {
		my $csize = $so->GetSetting('size') or $self->panic("$so : can't open 'size' object");
		push(@A,[3,sprintf("%s| %-64s | %s | %s", '#Id', 'Path', 'Size (MB)', '% Done')]);
		
		for(my $i=0; $i < $so->GetFileCount; $i++) {
			my $this_file    = $so->GetFileInfo($i);
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
			
			if($pcdone >= 100 && $done_chunks != $num_chunks) {
				$pcdone = 99.99;
			}
			
			my $msg = sprintf("%3d| %-64s | %8.2f  | %5.1f%%", 1+$i, $path, (($this_file->{size})/1024/1024), $pcdone);
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
	else {
		$NOEXEC .= "Usage: files queue_id [list | exclude fileId | include fileId] type 'help files' for more information";
	}
	return({MSG=>\@A, SCRAP=>[], NOEXEC=>$NOEXEC});
}


##########################################################################
# Create a new storage subdirectory
sub CreateStorage {
	my($self, %args) = @_;
	
	my $metadir = $self->_GetMetadir($args{StorageId});
	my $workdir = $self->{super}->Tools->GetExclusiveDirectory($self->_GetWorkdir, $self->_FsSaveDirent($args{StorageId}));
	
	if(-d $metadir) {
		$self->panic("$metadir exists!");
	}
	elsif(!defined($workdir)) {
		$self->panic("Failed to find an exclusive directory for $args{StorageId}");
	}
	else {
		mkdir($metadir) or $self->panic("Unable to mkdir($metadir) : $!");   # Create metaroot
		mkdir($workdir) or $self->panic("Unable to mkdir($workdir) : $!"); # Create StoreRoot
		my $flo  = delete($args{FileLayout});
		my $flb  = '';
		foreach my $flk (keys(%$flo)) {
			my @a_path = map($self->_FsSaveDirent($_), @{$flo->{$flk}->{path}}); # should be save now
			my $path   = join('/', @a_path );
			my $d_size = $flo->{$flk}->{end} - $flo->{$flk}->{start};
			$flb      .= "$path\0$flo->{$flk}->{start}\0$flo->{$flk}->{end}\n";
			
			# Create the dummy file itself:
			my $d_file = pop(@a_path);
			my $dir    = $workdir;
			foreach my $dirent (@a_path) {
				$dir .= "/".$dirent;
				next if -d $dir;
				mkdir($dir) or $self->panic("Failed to mkdir($dir) : $!");
			}
			$d_file = $dir."/".$d_file;
			
			# Create fake sparse file. (Note: We first create an off-by-one sparsefile because
			# truncate may fail if the file is < than $d_size)
			open(XF, ">", $d_file) or $self->panic("Failed to create sparsefile $d_file");
			seek(XF,  $d_size, 1)  or $self->panic("Failed to seek to $d_size: $!");
			syswrite(XF, 1, 1)     or $self->panic("Failed to write fakebyte: $!");
			truncate(XF, $d_size)  or $self->panic("Failed to truncate: $!");
			close(XF);
		}
		
		
		
		if(int($args{Size}/$args{Chunks}) > 0xFFFFFFFF) {
			# FIXME: Better check ## XAU
			$self->stop("Chunksize too big, storage plugin can't handle such big values");
		}
		
		my $xobject = Bitflu::StorageVFS::SubStore->new(_super => $self, sid => $args{StorageId} );
		
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
		$xobject->_SaveMetadata;
		return $self->OpenStorage($args{StorageId});
	}
}

##########################################################################
# Open an existing storage
sub OpenStorage {
	my($self, $sid) = @_;
	
	if(exists($self->{so}->{$sid})) {
		return $self->{so}->{$sid};
	}
	if(-d $self->_GetMetadir($sid)) {
		$self->{so}->{$sid} = Bitflu::StorageVFS::SubStore->new(_super => $self, sid => $sid );
		my $so = $self->OpenStorage($sid);
		$so->_UpdateExcludeList;                      # Cannot be done in new() because it needs a complete storage
		$so->_SetDataroot($so->GetSetting('path'));   # Same here
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
	
	my $base  = $self->{super}->Configuration->GetValue('workdir');
	my $temp  = $self->{super}->Configuration->GetValue('tempdir');
	my $sobj  = $self->OpenStorage($sid) or $self->panic("Cannot remove non-existing storage with sid=$sid");
	my @slist = $sobj->_ListSettings;
	my $xdir  = $self->{super}->Tools->GetExclusiveDirectory($base."/".$temp, $sobj->_GetSid) or $self->panic("Could not get exclusive dirname");
	
	rename($sobj->_GetMetadir, $xdir) or $self->panic("Cannot rename: $!");
	foreach my $key (@slist) {
		unlink($xdir."/".$key) or $self->panic("Cannot remove $xdir/$key: $!");
	}
	rmdir($xdir)                or $self->panic("Cannot remove $xdir : $!");
	delete($self->{so}->{$sid}) or $self->panic("Cannot remove socache entry");
	return 1;
}

sub ClipboardGet {
	my($self,$key) = @_;
	return $self->__ClipboardLoad->{$key};
}

sub ClipboardSet {
	my($self,$key,$value) = @_;
	my $cb = $self->__ClipboardLoad;
	$cb->{$key} = $value;
	return $self->__ClipboardStore($cb);
}

sub ClipboardRemove {
	my($self,$key) = @_;
	my $cb = $self->__ClipboardLoad;
	delete($cb->{$key}) or $self->panic("Cannot remove non-existing key '$key' from CB");
	return $self->__ClipboardStore($cb);
}
sub ClipboardList {
	my($self) = @_;
	my $cb = $self->__ClipboardLoad;
	return(keys(%$cb));
}

sub __ClipboardLoad {
	my($self) = @_;
	my ($cb, undef) = Bitflu::StorageVFS::SubStore::_ReadFile(undef,$self->__GetClipboardFile);
	return Storable::thaw($cb);
}

sub __ClipboardStore {
	my($self,$cb) = @_;
	return Bitflu::StorageVFS::SubStore::_WriteFile(undef, $self->__GetClipboardFile, Storable::nfreeze($cb));
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
	return $val;
}




sub debug { my($self, $msg) = @_; $self->{super}->debug("Storage : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Storage : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Storage : ".$msg); }
sub stop { my($self, $msg) = @_; $self->{super}->stop("Storage : ".$msg); }
sub warn { my($self, $msg) = @_; $self->{super}->warn("Storage : ".$msg); }


1;

package Bitflu::StorageVFS::SubStore;
use strict;
use constant MAXCACHE  => 256;         # Do not cache data above 256 bytes
use constant CHUNKSIZE => 1024*512;   # Must NOT be > than Network::MAXONWIRE;


sub new {
	my($class, %args) = @_;
	my $ssid = $args{_super}->_FsSaveStorageId($args{sid});
	my $self = {    _super => $args{_super},
	                   sid => $ssid,
	                scache => {},
	                    bf => { free => [], done => [], exclude => [], progress=>'' },
	                 fomap => [],
	           };
	bless($self,$class);
	
	# Cache file-layout
	my @fo      = split(/\n/, ( $self->GetSetting('filelayout') || '' ) ); # May not yet exist
	$self->{fo} = \@fo;
	
	# Build bitmasks:
	my $num_chunks = ($self->GetSetting('chunks') || 0)-1; # Can be -1
	my $c_size     = ($self->GetSetting('size'));
	
	# Init some internal stuff:
	$self->_InitBitfield($self->{bf}->{free}, $num_chunks);
	$self->_SetBitfield($self->{bf}->{done}, $self->GetSetting('bf_done'));
	$self->{bf}->{progress} = $self->GetSetting('bf_progress');
	
	# Build freelist from done information
	for(0..$num_chunks) {
		if(! $self->_GetBit($self->{bf}->{done}, $_)) {
			$self->_SetBit($self->{bf}->{free}, $_);
		}
	}
	
	# Fixme: Should be: $self->_BuildFoMap;
	my $fo_i = 0;
	foreach my $this_fo (@fo) {
		my($fo_path, $fo_start, $fo_end) = split(/\0/, $this_fo);
		my $piece_start = int($fo_start/$c_size);
		my $piece_end   = int($fo_end/$c_size);
		for($piece_start..$piece_end) {
			push(@{$self->{fomap}->[$_]}, $fo_i);
		}
		$fo_i++;
	}
	
	return $self;
}

sub _GetDataroot {
	my($self) = @_;
	return ( $self->GetSetting('path') or $self->panic("No dataroot?!") );
}

sub _SetDataroot {
	my($self, $path) = @_;
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
		$self->{scache}->{$key} = $val;
		return $self->_WriteFile($self->_GetMetadir."/$key",$val);
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
		($xval,$size) = $self->_ReadFile($self->_GetMetadir."/$key");
		$self->{scache}->{$key} = $xval if $size <= MAXCACHE;
	}
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
	print XFILE $value;
	close(XFILE);
	rename($tmpfile, $file) or $self->panic("Unable to rename $tmpfile into $file : $!");
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
	my $offset       = $args{Offset};
	my $length       = $args{Length};
	my $dataref      = $args{Data};
	my $chunk        = int($args{Chunk});
	my $foitems      = $self->{fomap}->[$chunk];
	my $strm_start   = $offset+($self->GetSetting('size')*$chunk);
	my $strm_end     = $strm_start+$length;
	my $chunk_border = ($self->GetSetting('size')*($chunk+1));
	my $didwrite     = 0;
	my $expct_offset= $offset+$length;
	
	$self->panic("Crossed pieceborder! ($strm_end > $chunk_border)") if $strm_end > $chunk_border;
	
	my $fox = {};
	foreach my $folink (@$foitems) {
		my $start = $self->GetFileInfo($folink)->{start};
		$fox->{$start} = $folink;
	}
	
	foreach my $xxx (sort({$a <=> $b} keys(%$fox))) {
			my $folink    = $fox->{$xxx};
			my $finf      = $self->GetFileInfo($folink);        # Get fileInfo hash
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
			
			open(THIS_FILE, "+<", $fp)                                 or $self->panic("Cannot open $fp for writing: $!");
			seek(THIS_FILE, $file_seek, 1)                             or $self->panic("Cannot seek to position $file_seek in $fp : $!");
			(syswrite(THIS_FILE, ${$dataref}, $canwrite) == $canwrite) or $self->panic("Short write in $fp: $!");
			close(THIS_FILE);
			
			${$dataref} = substr(${$dataref}, $canwrite);
			$length -= $canwrite;
			$self->panic() if $length < 0;
	}
	
	if($length) {
		$self->panic("Did not read requested data: length is set to $length (should be 0 bytes)");
	}
	substr($self->{bf}->{progress},$chunk*4,4,pack("N",$expct_offset));
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
		my $start = $self->GetFileInfo($folink)->{start};
		$fox->{$start} = $folink;
	}
	
	foreach my $xxx (sort({$a <=> $b} keys(%$fox))) {
			my $folink    = $fox->{$xxx};
			my $finf      = $self->GetFileInfo($folink);        # Get fileInfo hash
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
			
			open(THIS_FILE, "<", $fp)                       or $self->panic("Cannot open $fp for reading: $!");
			seek(THIS_FILE, $file_seek, 1)                  or $self->panic("Cannot seek to position $file_seek in $fp : $!");
			(sysread(THIS_FILE, $xb, $canread) == $canread) or $self->panic("Short read in $fp !");
			close(THIS_FILE);
			$buff   .= $xb;
			$length -= $canread;
	}
	
	if($length) {
		$self->panic("Did not read requested data: length is set to $length (should be 0 bytes)");
	}
	
	
	return $buff;
}



####################################################################################################################################################
# Misc stuff
####################################################################################################################################################

sub Truncate {
	my($self, $chunknum) = @_;
	$self->IsSetAsInwork($chunknum) or $self->panic;
	substr($self->{bf}->{progress},$chunknum*4,4,pack("N",0));
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
	$self->panic unless defined $bitnum;
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
			my $finfo = $self->GetFileInfo($i);
			my $first = int($finfo->{start}/$piecesize);
			my $last  = int($finfo->{end}/$piecesize);
			for($first..$last) { $self->_UnsetBit($ref_exclude,$_); }
		}
	}
}

sub GetExcludeHash {
	my($self) = @_;
	my $estr = $self->GetSetting('exclude');
	   $estr = '' unless defined($estr); # cannot use || because this would match '0'
	my %ex = map ({ int($_) => 1; } split(/,/,$estr));
	return \%ex;
}

sub GetExcludeCount {
	my($self) = @_;
	my $eh = $self->GetExcludeHash;
	return int(keys(%$eh));
}

sub _SetExcludeHash {
	my($self, $ref) = @_;
	my $str = join(',', keys(%$ref));
	$self->SetSetting('exclude',$str);
	$self->_UpdateExcludeList;
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
		return(undef, undef); #Hit EOF
	}
	else {
		my $canread = $fi->{size} - $offset;
		   $canread = ($canread > CHUNKSIZE ? CHUNKSIZE : $canread);
		my $thisp_start = int($fi->{start}/$psize);
		my $thisp_end   = int(($fi->{start}+$canread)/$psize);
		my $xsimulated  = 0;
		my $xb          = '';
		for($thisp_start..$thisp_end) {
			$xsimulated += (( ($self->IsSetAsDone($_)) ? 0 : 1 ) * $psize);
		}
		
		open(THIS_FILE, "<", $fp)                       or $self->panic("Cannot open $fp for reading: $!");
		seek(THIS_FILE, $offset, 0)                     or $self->panic("Cannot seek to offset $offset in $fp: $!");
		(sysread(THIS_FILE, $xb, $canread) == $canread) or $self->panic("Failed to read $canread bytes from $fp: $!");
		close(THIS_FILE);
		
		if($xsimulated) {
			$self->warn("Simulating $xsimulated bytes for file '$fp'");
		}
		
		return($xb, $xsimulated);
	}
	
	print "Chunk: $chunk at $offset ($fi->{size})\n";
	print Data::Dumper::Dumper($fi);
	
	return('',0);
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
# Returns true if file exists
sub GetFileCount {
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

