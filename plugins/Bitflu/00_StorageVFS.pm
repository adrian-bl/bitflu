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
use constant _BITFLU_APIVERSION => 20080505;
use constant BITFLU_METADIR     => '.bitflu-meta-do-not-touch';
use constant SAVE_DELAY         => 18;
##########################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, conf => {}, so => {}, nextsave => 0 };
	bless($self,$class);
	
	my $cproto = { incomplete_downloads => $mainclass->Configuration->GetValue('workdir')."/xsinc",
	               completed_downloads  => $mainclass->Configuration->GetValue('workdir')."/xsdone"
	             };
	
	foreach my $this_key (keys(%$cproto)) {
		my $this_value = $mainclass->Configuration->GetValue($this_key);
		if(!defined($this_value) or length($this_value) == 0) {
			$mainclass->Configuration->SetValue($this_key, $cproto->{$this_key});
		}
		$mainclass->Configuration->RuntimeLockValue($this_key);
	}
	
	# Shortcuts
	$self->{conf}->{storeroot} = $mainclass->Configuration->GetValue('incomplete_downloads');
	$self->{conf}->{finished}  = $mainclass->Configuration->GetValue('completed_downloads');
	$self->{conf}->{metas}     = $self->{conf}->{storeroot}."/".BITFLU_METADIR;
	
	return $self;
}

##########################################################################
# Init 'workdir'
sub init {
	my($self) = @_;
	$self->{super}->AddStorage($self);
	
	foreach my $this_dname qw(storeroot finished metas) {
		my $this_dir = $self->{conf}->{$this_dname};
		next if -d $this_dir;
		$self->warn("mkdir $this_dir");
		mkdir($this_dir) or $self->panic("Unable to create directory '$this_dir' : $!");
	}
	
	$self->{super}->AddRunner($self);
	return 1;
}

sub run {
	my($self) = @_;
	my $NOW = $self->{super}->Network->GetTime;
	
	return if $NOW < $self->{nextsave};
	$self->{nextsave} = $NOW + SAVE_DELAY;
	
	foreach my $sid (@{$self->GetStorageItems}) {
		$self->warn("Saving metadata of $sid");
		my $so = $self->OpenStorage($sid) or $self->panic("Unable to open $sid: $!");
		$so->_SaveMetadata;
	}
}

##########################################################################
# Save Bitfields
sub terminate {
	my($self) = @_;
	foreach my $sid (@{$self->GetStorageItems}) {
		$self->warn("Saving metadata of $sid");
		my $so = $self->OpenStorage($sid) or $self->panic("Unable to open $sid: $!");
		$so->_SaveMetadata;     # FIXME: Ugly code duplication
		bless($so, 'INVALID');
	}
}




##########################################################################
# Create a new storage subdirectory
sub CreateStorage {
	my($self, %args) = @_;
	
	my $root  = $self->_MetaRootPath($args{StorageId});
	my $sroot = $self->_StoreRootPath."/".$self->_FsSaveDirent($args{StorageId});
	
	if(-d $root || -d $sroot) {
		$self->warn("$root exists, won't re-create the same storage dir");
		return undef;
	}
	else {
		mkdir($root)  or $self->panic("Unable to mkdir($root) : $!");   # Create metaroot
		mkdir($sroot) or $self->panic("Unable to create($sroot) : $!"); # Create StoreRoot
		my $flo  = delete($args{FileLayout});
		my $flb  = '';
		foreach my $flk (keys(%$flo)) {
			my @a_path = map($self->_FsSaveDirent($_), @{$flo->{$flk}->{path}}); # should be save now
			my $path   = join('/', @a_path );
			my $d_size = $flo->{$flk}->{end} - $flo->{$flk}->{start};
			$flb      .= "$path\0$flo->{$flk}->{start}\0$flo->{$flk}->{end}\n";
			
			# Create the dummy file itself:
			my $d_file = pop(@a_path);
			my $dir    = $sroot;
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
		$xobject->SetSetting('storeroot',  $sroot);
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
	if(-d $self->_MetaRootPath($sid)) {
		$self->{so}->{$sid} = Bitflu::StorageVFS::SubStore->new(_super => $self, sid => $sid );
		return $self->OpenStorage($sid);
	}
	else {
		return 0;
	}
}

##########################################################################
# Kill existing storage directory
sub RemoveStorage {
	my($self, $sid) = @_;
	$self->panic;
}

sub ClipboardGet {
	my($self,$key) = @_;
	$self->warn("XAU: ClipboardGet($key) called, returning undef");
	return undef;
}

sub ClipboardSet {
	my($self,$key,$value) = @_;
	$self->warn("XAU: ClipboardSet($key,$value) is a fake");
	return undef;
}

sub ClipboardRemove {
	my($self,$key) = @_;
	$self->panic;
}
sub ClipboardList {
	my($self) = @_;
	$self->panic;
}


##########################################################################
# Returns path to metas directory
sub _Metadir {
	my($self) = @_;
	return $self->{conf}->{metas};
}

sub _MetaRootPath {
	my($self,$sid) = @_;
	return $self->_Metadir."/".$self->_FsSaveStorageId($sid);
}

sub _StoreRootPath {
	my($self) = @_;
	return $self->{conf}->{storeroot};
}


##########################################################################
# Returns an array of all existing storage directories
sub GetStorageItems {
	my($self) = @_;
	my $storeroot = $self->_Metadir;
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
use constant MAXCACHE     => 256;         # Do not cache data above 256 bytes


sub new {
	my($class, %args) = @_;
	my $ssid = $args{_super}->_FsSaveStorageId($args{sid});
	my $self = {    _super => $args{_super},
	                   sid => $ssid,
	              rootpath => $args{_super}->_MetaRootPath($ssid),
	              datapath => $args{_super}->_StoreRootPath."/$ssid",
	                scache => {},
	                    bf => { free => [], done => [], progress=>'' },
	                 fomap => [],
	           };
	bless($self,$class);
	
	# Cache file-layout
	my @fo      = split(/\n/, ( $self->GetSetting('filelayout') || '' ) ); # May not yet exist
	$self->{fo} = \@fo;
	
	# Build bitmasks:
	my $num_chunks = ($self->GetSetting('chunks') || 0)-1; # Can be -1
	my $c_size     = ($self->GetSetting('size'));
	
	$self->_InitBitfield($self->{bf}->{free}, $num_chunks);
	$self->_SetBitfield($self->{bf}->{done}, $self->GetSetting('bf_done'));
	
	$self->{bf}->{progress} = $self->GetSetting('bf_progress');
	
	# Build freelist from done information
	for(0..$num_chunks) {
		if(! $self->_GetBit($self->{bf}->{done}, $_)) {
			$self->_SetBit($self->{bf}->{free}, $_);
		}
	}
	
	
	# Should be: $self->_BuildFoMap;
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
	$self->warn("XAU: Commit is never done");
	return 0;
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
		return $self->_WriteFile($self->{rootpath}."/$key",$val);
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
		($xval,$size) = $self->_ReadFile($self->{rootpath}."/$key");
		$self->{scache}->{$key} = $xval if $size <= MAXCACHE;
	}
	return $xval;
}


##########################################################################
# Removes an item from .settings
sub RemoveSetting {
	my($self,$key) = @_;
	$self->panic;
}

sub ListSettings {
	my($self) = @_;
	$self->panic;
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
	my $offset      = $args{Offset};
	my $length      = $args{Length};
	my $dataref     = $args{Data};
	my $chunk       = int($args{Chunk});
	my $foitems     = $self->{fomap}->[$chunk];
	my $strm_start  = $offset+($self->GetSetting('size')*$chunk);
	my $strm_end    = $strm_start+$length;
	my $didwrite    = 0;
	my $expct_offset= $offset+$length;
	print "=====---[WriteRequest: Offset=$offset, Length=$length, Chunk=$chunk, ChunkSize=".$self->GetSetting('size').", StreamStart=>$strm_start]---====\n";
	
	# Fixme: Wir sollten checken, ob length() von data im chunk überhaupt platz hat
	
	my $fox = {};
	foreach my $folink (@$foitems) {
		my $start = $self->GetFileInfo($folink)->{start};
		$fox->{$start} = $folink;
		print "+$start\n";
	}
	
	foreach my $xxx (sort({$a <=> $b} keys(%$fox))) {
			my $folink    = $fox->{$xxx};
			my $finf      = $self->GetFileInfo($folink);        # Get fileInfo hash
			my $file_seek = 0;                                  # Seek to this position in file
			my $canwrite  = $length;                            # How much data we'll write
			my $fp        = $self->{datapath}."/$finf->{path}"; # Full Path
			
			if($strm_start > $finf->{start}) {
				# Requested data does not start at offset 0 -> Seek in file
				$file_seek = $strm_start - $finf->{start};
			}
			
			if($file_seek > $finf->{size}) {
				$self->warn("You should not seek behind borders! BUGBUG");
				next;
			}
			elsif($canwrite > ($finf->{size}-$file_seek)) {
				print "Must truncate! $canwrite > ($finf->{size}-$file_seek) -> ".($finf->{size}-$file_seek)."\n";
				$canwrite = ($finf->{size}-$file_seek); # Cannot read so much data..
			}
			
			print ">> Will write $canwrite bytes to $fp (aka $folink), starting at $file_seek (STREAMPOS: $finf->{start})\n";
			
			open(THIS_FILE, "+<", $fp)                                 or $self->panic("Cannot open $fp for writing: $!");
			seek(THIS_FILE, $file_seek, 1)                             or $self->panic("Cannot seek to position $file_seek in $fp : $!");
			(syswrite(THIS_FILE, ${$dataref}, $canwrite) == $canwrite) or $self->panic("Short write in $fp: $!");
			close(THIS_FILE);
			
			${$dataref} = substr(${$dataref}, $canwrite);
			$length -= $canwrite;
			print "left to go: $length bytes\n";
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
	my $didread     = 0;
	my $buff        = '';
	
	print "=====---[ReadRequest: Offset=$offset, Length=$length, Chunk=$chunk, ChunkSize=".$self->GetSetting('size').", StreamStart=>$strm_start]---====\n";
	
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
			my $fp        = $self->{datapath}."/$finf->{path}"; # Full Path
			my $xb        = '';                                 # Buffer for sysread output
			if($strm_start > $finf->{start}) {
				# Requested data does not start at offset 0 -> Seek in file
				$file_seek = $strm_start - $finf->{start};
			}
			
			if($canread > ($finf->{size}-$file_seek)) {
				$canread = ($finf->{size}-$file_seek); # Cannot read so much data..
			}
			
			next if $canread < 0; # XAU?!
			
			print ">> Will read $canread bytes from $fp (aka $folink), starting at $file_seek (STREAMPOS: $finf->{start})\n";
			
			open(THIS_FILE, "<", $fp)                       or $self->panic("Cannot open $fp for reading: $!");
			seek(THIS_FILE, $file_seek, 1)                  or $self->panic("Cannot seek to position $file_seek in $fp : $!");
			(sysread(THIS_FILE, $xb, $canread) == $canread) or $self->panic("Short read in $fp !");
			close(THIS_FILE);
			$buff   .= $xb;
			$length -= $canread;
			print "left to go: $length bytes\n";
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
	$self->warn("XAU IsSetAsExcluded");
	return 0;
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

sub GetExcludeHash {
	my($self) = @_;
	$self->panic;
}

sub GetExcludeCount {
	my($self) = @_;
	$self->warn("XAU: FAKE");
	return 0;
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
	
	$self->panic;
}

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

