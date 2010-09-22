package Bitflu::DownloadBitTorrent;
#
# This file is part of 'Bitflu' - (C) 2006-2010 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#

# Fixme: Warum locken wir das piece in StoreData nicht, wenn es frei war?
# Fixme: Beim TIMEOUT_FAST sollten wir ggf. ein HuntPiece irgendwie nachschieben
#

use strict;
use List::Util;
use constant _BITFLU_APIVERSION => 20100424;

use constant SHALEN   => 20;
use constant BTMSGLEN => 4;

use constant BUILDID => 'A811';  # YMDD (Y+M => HEX)

use constant STATE_READ_HANDSHAKE    => 200;  # Wait for clients Handshake
use constant STATE_READ_HANDSHAKERES => 201;  # Read clients handshake response
use constant STATE_NOMETA            => 299;  # No meta data received (yet)
use constant STATE_IDLE              => 300;  # Connection with client fully established


use constant MSG_CHOKE          => 0;  # Implemented
use constant MSG_UNCHOKE        => 1;  # Implemented
use constant MSG_INTERESTED     => 2;  # Implemented
use constant MSG_UNINTERESTED   => 3;  # Implemented
use constant MSG_HAVE           => 4;  # Implemented
use constant MSG_BITFIELD       => 5;  # Implemented
use constant MSG_REQUEST        => 6;  # Implemented
use constant MSG_PIECE          => 7;  # Implemented
use constant MSG_CANCEL         => 8;  # Implemented
use constant MSG_PORT           => 9;
use constant MSG_WANT_METAINFO  => 10; # Unused
use constant MSG_METAINFO       => 11; # Unused
use constant MSG_SUSPECT_PIECE  => 12; # Unused
                                       # 13-17 was FastPeers extension. But nobody supports it anyway.. :-)
use constant MSG_HOLE_PUNCH     => 18; # NAT-Unused
use constant MSG_UTORRENT_MSG   => 19; # Unused (??)
use constant MSG_EPROTO         => 20;

use constant TIMEOUT_NOOP          => 110;    # Ping each 110 seconds
use constant TIMEOUT_FAST          => 20;     # Fast timeouter (wait for bitfield, handshake, etc)
use constant TIMEOUT_UNUSED_CLIENT => 1200;   # Drop connection if we didn't send/recv a piece within 20 minutes ('deadlock' connection)
use constant TIMEOUT_PIECE_NORM    => 90;     # How long we are going to wait for a piece in 'normal' mode
use constant TIMEOUT_PIECE_FAST    => 8;      # How long we are going to wait for a piece in 'almost done' mode

use constant DELAY_FULLRUN         => 26;     # How often we shall save our configuration and rebuild the have-map
use constant DELAY_CHOKEROUND      => 30;     # How often shall we run the unchoke round?
use constant EP_HANDSHAKE          => 0;
use constant EP_UT_PEX             => 1;
use constant EP_UT_METADATA        => 2;

use constant PEX_MAXPAYLOAD        => 32;    # Limit how many clients we are going to send
use constant MKTRNT_MINPSIZE       => 32768; # Min chunksize to use for torrents. Note: uTorrent cannot handle any smaller files!

use constant MIN_LASTQRUN          => 5;     # Wait at least 5 seconds before doing a new queue run
use constant MIN_HASHFAILS         => 3;     # Only blacklist if we got AT LEAST this many hashfails

use constant UTMETA_MAXSIZE        => 10*1024*1024; # Space to allocate for ut_meta

use constant CONN_OUT_MINPORT      => 1024;  # do not connect to ports below or eq to this

use fields qw( super phunt verify verify_task Dispatch CurrentPeerId ownip );

##########################################################################
# Register BitTorrent support
sub register {
	my($class, $mainclass) = @_;
	my $ptype = { super => $mainclass, phunt => { phi => 0, phclients => [], lastchokerun => 0, lastqrun => 0, dqueue => {},
	                                             fullrun => 0, chokemap => { can_choke => {}, can_unchoke => {}, optimistic => 0, seedprio=>{} },
	                                             havemap => {}, pexmap => {} },
	             verify => {}, verify_task=>undef,
	             ownip => { ipv4=>undef, ipv6=>undef },
	           };
	
	my $self = fields::new($class);
	map( $self->{$_} = delete($ptype->{$_}), keys(%$ptype) );
	
	$self->{Dispatch}->{Torrent} = Bitflu::DownloadBitTorrent::Torrent->new(super=>$mainclass, _super=>$self);
	$self->{Dispatch}->{Peer}    = Bitflu::DownloadBitTorrent::Peer->new(super=>$mainclass, _super=>$self);
	$self->{CurrentPeerId}       = pack("H*",unpack("H40", "-BF".BUILDID."-".sprintf("#%X%X",int(rand(0xFFFFFFFF)),int(rand(0xFFFFFFFF)))));
	
	my $cproto = { torrent_port => 6688, torrent_bind => 0, torrent_maxpeers => 80,
	               torrent_upslots => 10, torrent_importdir => $mainclass->Configuration->GetValue('workdir').'/import',
	               torrent_gcpriority => 8, torrent_totalpeers => 400, torrent_maxreq => 6 };
	
	foreach my $funk qw(torrent_maxpeers torrent_gcpriority torrent_upslots torrent_maxreq) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		unless(defined($this_value)) {
			$mainclass->Configuration->SetValue($funk, $cproto->{$funk});
		}
	}
	
	foreach my $funk qw(torrent_port torrent_bind torrent_totalpeers torrent_importdir) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		unless(defined($this_value)) {
			$mainclass->Configuration->SetValue($funk,$cproto->{$funk});
		}
		$mainclass->Configuration->RuntimeLockValue($funk);
	}
	
	
	my $main_socket = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$mainclass->Configuration->GetValue('torrent_port'),
	                                                    Bind=>$mainclass->Configuration->GetValue('torrent_bind'),
	                                                    MaxPeers=>$mainclass->Configuration->GetValue('torrent_totalpeers'),
	                                                    Callbacks => {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	
	if($main_socket) {
		$mainclass->AddRunner($self);
		return $self;
	}
	else {
		$self->stop("Unable to listen on ".$mainclass->Configuration->GetValue('torrent_bind').":".$mainclass->Configuration->GetValue('torrent_port')." : $!");
	}
}

##########################################################################
# Regsiter admin commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('bt_connect', $self, '_Command_CreateConnection', "Creates a new bittorrent connection",
	[ [undef, "Usage: bt_connect queue_id ip port"],
	  [undef, ""],
	  [undef, "This command can be used to forcefully establish a connection with a known peer"]
	]);
	$self->{super}->Admin->RegisterCommand('load', $self, 'LoadTorrentFromDisk'              , "Start downloading a new .torrent file",
	[ [undef, "1: Store the torrent file in a directory readable by bitflu (watchout for chroot and permissions)"],
	  [undef, "2: Type: 'load /patch/to/torrent.torrent'                   (avoid whitespaces)"],
	  [undef, "Hint: You can also place torrent into the 'autoload' folder. Bitflu will pickup the files itself"],
	] );
	
	$self->{super}->Admin->RegisterCommand('import_torrent', $self, '_Command_ImportTorrent', 'ADVANCED: Import torrent from torrent_importdir');
	$self->{super}->Admin->RegisterCommand('create_torrent', $self, '_Command_CreateTorrent', 'ADVANCED: Create .torrent-file from torrent_importdir',
	[ [undef, "Usage: create_torrent --name [--tracker http://example.com] [--private]"],
	  [undef, ''],
	  [3, 'The create_torrent creates a new .torrent file using data stored in \'torrent_importdir\'.'],
	  [3, 'Please note that bitflu blocks until all data has been hashed and imported:'],
	  [3, 'this may take up to a few minutes.'],
	  [undef, ''],
	  [1, 'Possible arguments:'],
	  [undef, '--name      : Name of the file to create'],
	  [undef, '--tracker   : Tracker to use. Use \',\' to seperate multiple trackers and \'!\' to form groups'],
	  [undef, '--private   : If set, torrent is marked as private (disables DHT)'],
	  [undef, '--comment   : Add a comment to the .torrent file'],
	  [undef, ''],
	  [1, 'Examples:'],
	  [1,     'create_torrent --name example --tracker http://example.com/foobar'],
	  [undef, ' -> Creates a torrent named "example" that uses "http://example.com/foobar" as tracker.'],
	  [undef, ''],
	  [1,     'create_torrent --name example --tracker http://foo.com,http://bar.com!http://foo2.com'],
	  [undef, ' -> Creates a torrent with 3 trackers. "bar.com" and "foo2.com" will be in the same group'],
	  [undef, ''],
	  [1,     'create_torrent --name example'],
	  [undef, ' -> Creates a torrent without any trackers. Will only work on DHT-Enabled clients (such as bitflu)'],
	  [undef, ''],
	  [2,     'Note to people who create singlefile torrent:'],
	  [2,     ' - Bitflu will replace the value of \'--name\' with the filename found at \'torrent_importdir\''],
	  [2,     ' - Do not put the file into a subdirectory because bitflu would be unable to autoimport such a file'],
	]);
	$self->{super}->Admin->RegisterCommand('analyze_torrent', $self, '_Command_AnalyzeTorrent', 'ADVANCED: Print decoded torrent information (excluding pieces)');
	
	$self->{super}->Admin->RegisterCommand('verify', $self, '_Command_VerifyTorrent', "Check download for corruptions",
	[ [undef, "Usage: verify [queue_id | progress]"],
	  [undef, ""],
	  [undef, "This command verifies the integrity of your download after a hard computer crash"],
	  [undef, "Use 'verify progress' to show the status of all verify processes"],
	]);
	
	$self->{super}->Admin->RegisterCommand('seedprio', $self, '_Command_SeedPriority', "Changes uploading/seeding priority of a torrent",
	[ [undef, "Usage: seedprio queue_id [VALUE]"],
	  [undef, ""],
	  [undef, "Changes seeding priority. Example"],
	  [undef, "seedprio queue_id 3    : Unchoke (at least) 3 peers"],
	  [undef, "seedprio queue_id 0    : Use bitflus autounchoker (default)"],
	  [undef, "seedprio queue_id      : Display configured value"],
	]);
	
	$self->{super}->Admin->RegisterCommand('seedhide', $self, '_Command_SeedHide', "Hide pieces from leechers while seeding a torrent",
	[ [undef, "Usage: seedhide queue_id [VALUE]"],
	  [undef, ""],
	  [undef, "Bitflu can hide the fact that it is seeding a torrent,"],
	  [undef, "this method might help if bitflu is the initial (and only)"],
	  [undef, "seeder of a new torrent."],
	  [undef, ""],
	  [undef, "An example:"],
	  [undef, "seedhide queue_id 30    : Tells bitflu to hide about 30%"],
	  [undef, "seedhide queue_id 0     : Normal value: hide no pieces"],
	]);
	
	$self->warn("DESTROY COMMAND IS ACTIVE");
	$self->{super}->Admin->RegisterCommand('destroy', $self, '_Command_Destroy', 'DEBUG: DESTROY RANDOM PIECES');
	
	unless(-d $self->{super}->Configuration->GetValue('torrent_importdir')) {
		$self->debug("Creating torrent_importdir '".$self->{super}->Configuration->GetValue('torrent_importdir')."'");
		mkdir($self->{super}->Configuration->GetValue('torrent_importdir')) or $self->panic("Unable to create torrent_importdir : $!");
	}
	
	
	$self->info("BitTorrent plugin loaded. Using tcp port ".$self->{super}->Configuration->GetValue('torrent_port'));
	return 1;
}

##########################################################################
# Establish a new Torrent connection
sub _Command_CreateConnection {
	my($self, @args) = @_;
	
	my($hash, $ip, $port) = @args;
	my @MSG               = ();
	
	if($port && $ip) {
		$self->CreateNewOutgoingConnection($hash, $ip, $port);
		push(@MSG, [1, "Connection to torrent://$hash/$ip:$port established (maybe)"]);
	}
	else {
		push(@MSG, [2, "Usage: bt_connect hash ip port"]);
	}
	return({MSG=>\@MSG, SCRAP=>[]});
}


##########################################################################
# Creates a new .torrent file from data at importdir
sub _Command_CreateTorrent {
	my($self, @args) = @_;
	
	my $getopts      = $self->{super}->Tools->GetOpts(\@args);
	my $trnt_name    = delete($getopts->{name}) || '';
	my $trnt_ref     = { info => {  name=>$trnt_name, files => [], 'piece length' => undef, pieces => '' },
	                     announce => undef, 'announce-list' => [], private => int(exists($getopts->{private})),
	                    'creation date' => int(time()),
	                    'created by'    => 'Bitflu-'.BUILDID,
	                    'comment'       => ($getopts->{comment} || ''),
	                   };
	my $trnt_importd = $self->{super}->Configuration->GetValue('torrent_importdir');
	my $trnt_tmpfile = $self->{super}->Tools->GetExclusiveTempfile;
	my $trnt_rawlist = { list => [] };
	my $trnt_size    = 0;
	my $trnt_plength = undef;
	my $scratch_buff = '';
	my $scratch_len  = 0;
	my @MSG          = ();
	
	#Build announce-list and announce
	foreach my $chunk (split(',',$getopts->{tracker}||'')) {
		my @chunklist = split('!', $chunk);
		push(@{$trnt_ref->{'announce-list'}}, \@chunklist);
	}
	$trnt_ref->{announce} = $trnt_ref->{'announce-list'}->[0]->[0];
	
	# Delete unneeded elements
	if( ($#{$trnt_ref->{'announce-list'}}+$#{$trnt_ref->{'announce-list'}->[0]}) < 1 ) {
		delete($trnt_ref->{'announce-list'});
	}
	unless(($trnt_ref->{announce})) {
		delete($trnt_ref->{'announce'});
	}
	
	# Ditch comment if empty
	delete($trnt_ref->{comment}) if length($trnt_ref->{comment}) == 0;
	
	# Fill in $trnt_ref->{info}->{files}
	$self->{super}->Tools->GenDirList($trnt_rawlist, $trnt_importd);
	foreach my $dirent (sort(@{$trnt_rawlist->{list}})) {
		next if -d $dirent;
		next if -l $dirent; # Do not import symlinked files (security reasons)
		my $internal_raw = substr($dirent,length($trnt_importd)+1);
		my @internal_a   = split('/',$internal_raw);
		my $this_ref = { path => \@internal_a, length => (-s $dirent), fp=>$dirent };
		push(@{$trnt_ref->{info}->{files}}, $this_ref);
		$trnt_size += $this_ref->{length};
	}
	
	# Abort if we are missing something:
	if(length($trnt_name) == 0 or $trnt_size < 1) {
		push(@MSG, [2, "Usage error, type 'help create_torrent' for more information"]);
		return({MSG=>\@MSG, SCRAP=>[]});
	}
	
	
	# We can now calculate a piece-length
	$trnt_plength = int(sqrt($trnt_size)*32);                                      # Guess a piece-size
	$trnt_plength = ($trnt_plength > (2**23)         ? (2**23)         : $trnt_plength);  # -> Do not go above 8mb
	$trnt_plength = ($trnt_plength < MKTRNT_MINPSIZE ? MKTRNT_MINPSIZE : $trnt_plength);  # -> and not to small..
	$trnt_plength = ($trnt_size    < $trnt_plength   ? $trnt_size      : $trnt_plength);  # -> and not above the actual file size (if < 1024)
	$trnt_ref->{info}->{'piece length'} = $trnt_plength; # Fixup the reference
	
	foreach my $this_ref (@{$trnt_ref->{info}->{files}}) {
		next if $trnt_size == 0; # Skip empty junk-files at end of list
		my $this_path  = delete($this_ref->{fp});
		my $bytes_left = $this_ref->{length};
		open(HFILE, "<", $this_path) or $self->panic("Unable to hash $this_path");
		for(;;) {
			my $xbuffer      = '';
			my $scratch_left = $trnt_plength - $scratch_len;
			my $must_read    = ($bytes_left   < $trnt_plength ? $bytes_left   : $trnt_plength);
			   $must_read    = ($scratch_left < $must_read    ? $scratch_left : $must_read);
			my $xread        = read(HFILE, $xbuffer, $must_read);
			if(!defined($xread) or $must_read != $xread) {
				$self->panic("Failed to read $must_read bytes from $this_path : $!");
			}
			$scratch_buff .= $xbuffer;
			$scratch_len  += $xread;
			$trnt_size    -= $xread;
			$bytes_left   -= $xread;
			
			if($scratch_len == $trnt_plength or $trnt_size == 0) { # Buffer is full or/and torrent ended
				$self->warn("Hashing $scratch_len bytes of $this_path, $trnt_size bytes left to go...");
				my $sha1 = $self->{super}->Tools->sha1($scratch_buff);
				$trnt_ref->{info}->{pieces} .= $sha1;
				$scratch_len  = 0;
				$scratch_buff = '';
			}
			last if $bytes_left == 0;
		}
		close(HFILE);
	}
	
	if($#{$trnt_ref->{info}->{files}} == 0) {
		# Convert multifile-torrent into a maketorrent-console-style singlefile torrent
		$trnt_ref->{info}->{length} = $trnt_ref->{info}->{files}->[0]->{length};
		$trnt_ref->{info}->{name}   = $trnt_ref->{info}->{files}->[0]->{path}->[-1];
		delete($trnt_ref->{info}->{files});
	}
	
	
	my $this_sha1 = $self->{super}->Tools->sha1_hex($self->{super}->Tools->BencEncode($trnt_ref->{info}));
	my $this_benc = $self->{super}->Tools->BencEncode($trnt_ref);
	my $this_dest = $self->{super}->Tools->GetTempdir."/torrent-$this_sha1.torrent";
	open(TFILE, ">", $trnt_tmpfile) or $self->panic("Unable to write to $trnt_tmpfile: $!");
	print TFILE $this_benc;
	close(TFILE);
	
	# rename to a nicer name:
	rename($trnt_tmpfile, $this_dest) or $self->panic("Ouch! Could not move $trnt_tmpfile to $this_dest : $!"); # cheap
	
	# Try to autoload it:
	$self->{super}->Admin->ExecuteCommand('history', $this_sha1, 'forget');
	$self->{super}->Admin->ExecuteCommand('load',    $this_dest);
	$self->{super}->Admin->ExecuteCommand('import_torrent', $this_sha1);
	
	push(@MSG, [undef, "torrent created. A copy of the .torrent file is stored at $this_dest [sha1: $this_sha1]"]);
	
	return({MSG=>\@MSG, SCRAP=>[]});
}

##########################################################################
# Import a torrent from disk
sub _Command_ImportTorrent {
	my($self, $sha1) = @_;
	
	my @A       = ();
	my $so      = $self->{super}->Storage->OpenStorage($sha1);
	my $pfx     = $self->{super}->Configuration->GetValue('torrent_importdir');
	
	
	if($so) {
		my $torrent   = $self->Torrent->GetTorrent($sha1) or $self->panic("Unable to open torrent for $sha1");
		my $cs        = $so->GetSetting('size') or $self->panic("$sha1 has no size setting");
		my $fl        = ();
		my $fake_upld = $self->{super}->Queue->GetStats($sha1)->{done_bytes};
		my $fake_peer = $self->Peer->AddNewClient($self, {Port=>0, RemoteIp=>'0.0.0.0'});
		
		$fake_peer->SetSha1($sha1);
		$fake_peer->SetBitfield(pack("B*", ("1" x length(unpack("B*",$torrent->GetBitfield)))));
		
		for(my $i=0; $i < $so->GetFileCount; $i++) {
			my $this_file  = $so->GetFileInfo($i);
			my @a_clean    = ();
			my $path_raw   = $pfx;
			my $path_clean = $pfx;
			# Get a clean path
			foreach(split('/',$this_file->{path})) { next if $_ eq ".."; next if length($_) == 0; push(@a_clean,$_); }
			
			$path_raw   .= '/'.$this_file->{path};
			$path_clean .= '/'.join('/',@a_clean);
			
			my $i_raw   = ( (stat($path_raw))[1]   || 0);
			my $i_clean = ( (stat($path_clean))[1] || 0);
			
			if($i_raw == 0 && $i_clean == 0) {
				$self->warn("import: '$path_clean' does not exist, skipping file");
			}
			elsif($i_raw != $i_clean) {
				$self->warn("import: Obscure path '$path_raw' doesn't point to the same file as '$path_clean', skipping file");
			}
			elsif($this_file->{start} != $this_file->{end}) {
				$fl->{$this_file->{start}} = { path=>$path_clean, start=>$this_file->{start}, end=>$this_file->{end} };
			}
		}
		
		# Need to sort this, because we can only do streams
		foreach my $ckey (sort({ $a <=> $b} keys(%$fl))) {
			my $r = $fl->{$ckey};
			for(my $i = $r->{start}; $i < $r->{end};) {
				my $piece_to_use = int($i/$cs);
				my $piece_offset = $i - $piece_to_use*$cs;
				my $canread      = (($r->{end}-$i)    < $cs       ? ($r->{end}-$i)      : $cs);
				   $canread      = ($cs-$piece_offset < $canread ? ($cs-$piece_offset) : $canread);
				$i+=$canread;
				if ($so->IsSetAsFree($piece_to_use) && open(FEED, "<", $r->{path}) ) {
					$self->warn("Importing from local disk: Piece=>$piece_to_use, Size=>$canread, Offset=>$piece_offset, Path=>$r->{path}");
					my $buff = '';
					my $fail = 0;
					seek(FEED, $i-$canread-$r->{start},0)      or $fail = "error while seeking: $!";
					my $didread = sysread(FEED,$buff,$canread) or $fail = "short sysred (EOF?!) wanted $canread bytes";
					close(FEED);
					
					if($fail) {
						$self->warn("Failed to import data from $r->{path} : $fail");
						last;
					}
					else {
						$fake_peer->LockPiece(Index=>$piece_to_use, Offset=>$piece_offset, Size=>$didread);
						$so->Truncate($piece_to_use) if $piece_offset == 0;
						$fake_peer->StoreData(Index=>$piece_to_use, Offset=>$piece_offset, Size=>$didread, Dataref=>\$buff);
						$i -= ($canread-$didread); # Ugly ugly ugly.. but it's 23:39:43 ...
					}
					
				}
			}
		}
		$self->_Network_Close($self);
		# Calculate and set faked upload:
		$fake_upld = ($self->{super}->Queue->GetStats($sha1)->{done_bytes} - $fake_upld);
		$self->{super}->Queue->IncrementStats($sha1, { uploaded_bytes => $fake_upld } );
		$self->{super}->Admin->ExecuteCommand('autocommit', $sha1, 'off');
		$self->{super}->Admin->ExecuteCommand('autocancel', $sha1, 'off');
		push(@A, [1, "$sha1 : Import finished: imported $fake_upld bytes and disabled autocancel and autocommit."]);
	}
	else {
		push(@A, [2, "'$sha1' does not exist"]);
	}
	
	return({MSG=>\@A, SCRAP=>[] });
}

##########################################################################
# Print some details about a torrent
sub _Command_AnalyzeTorrent {
	my($self, $sha1) = @_;
	
	my @MSG   = ();
	my @SCRAP = ();
	my $torrent = undef;
	my $raw     = '';
	if($sha1 && ($self->Torrent->ExistsTorrent($sha1) && ($torrent = $self->Torrent->GetTorrent($sha1))) && $torrent->GetMetaSize) {
		my $decoded = $self->{super}->Tools->BencDecode($torrent->GetMetaData);
		delete($decoded->{pieces});
		foreach(split(/\n/,Data::Dumper::Dumper($decoded))) {
			push(@MSG, [0, $_]);
		}
	}
	else {
		push(@SCRAP,$sha1);
	}
	return({MSG=>\@MSG, SCRAP=>\@SCRAP});
}

##########################################################################
# Kick torrent verification
sub _Command_VerifyTorrent {
	my($self, @args) = @_;
	my @MSG   = ();
	my @SCRAP = ();
	my $torrent = undef;
	
	foreach my $sha1 (@args) {
		if(exists($self->{verify}->{$sha1})) {
			push(@MSG, [2, "$sha1: Verification is still running (at piece $self->{verify}->{$sha1}->{piece})"]);
		}
		elsif($sha1 && ($self->Torrent->ExistsTorrent($sha1) && ($torrent = $self->Torrent->GetTorrent($sha1)) && $torrent->GetMetaSize)) {
			push(@MSG, [0, "Starting verification of $sha1"]);
			$self->{verify}->{$sha1} = { sid=>$sha1, torrent=>$torrent, piece=>0, bad=>{} };
			$self->{verify_task} ||= $self->{super}->CreateSxTask(Superclass=>$self,Callback=>'RunVerification', Args=>[5]);
		}
		elsif($sha1 eq 'progress') {
			push(@MSG, [3, "Verification progress:"]);
			foreach my $xref (values(%{$self->{verify}})) {
				push(@MSG, [1, "$xref->{sid} is at piece $xref->{piece} (".int(keys(%{$xref->{bad}}))." BAD piece(s) so far)"]);
			}
		}
		else {
			push(@SCRAP,$sha1);
		}
	}
	
	push(@MSG, [2, "Usage error: see 'help verify' for more information"]) unless int(@args);
	return({MSG=>\@MSG, SCRAP=>\@SCRAP});
}


##########################################################################
# Print some details about a torrent
sub _Command_SeedPriority {
	my($self, $sha1, $val) = @_;
	
	my @MSG   = ();
	my @SCRAP = ();
	my $torrent = undef;
	
	if($sha1 && $self->Torrent->ExistsTorrent($sha1)) {
		my $torrent = $self->Torrent->GetTorrent($sha1);
		if(defined($val) && $val =~ /^\d+$/) {
			$torrent->SetSeedPriority($val);
		}
		
		my $spval = $torrent->GetSeedPriority;
		
		push(@MSG,[1, "$sha1: seeding priority set to $spval".($spval == 0 ? ' (Automatic)' : '')]);
	}
	else {
		push(@SCRAP,$sha1);
	}
	return({MSG=>\@MSG, SCRAP=>\@SCRAP});
}

##########################################################################
# Fake Bitfield operations
sub _Command_SeedHide {
	my($self, $sha1, $val) = @_;
	
	my @MSG   = ();
	my @SCRAP = ();
	my $torrent = undef;
	
	if($sha1 && $self->Torrent->ExistsTorrent($sha1)) {
		my $torrent = $self->Torrent->GetTorrent($sha1);
		
		if($torrent->IsComplete) {
			if(defined($val) && $val =~ /^\d+$/) {
				if($val >= 0 && $val <= 95) {
					$torrent->Storage->SetSetting('_piecehide',$val);
					$torrent->RebuildFakeBitfield;
				}
				else {
					push(@MSG, [2, "$sha1: value '$val' out of range (must be 0-95)"]);
				}
			}
			
			$val = $torrent->Storage->GetSetting('_piecehide');
			push(@MSG,[1, "$sha1: ".($val?"~ $val% hidden" : "nothing hidden")]);
		}
		else {
			push(@MSG,[2, "$sha1: torrent not completed, command is disabled"]);
		}
		
	}
	else {
		push(@SCRAP,$sha1);
	}
	return({MSG=>\@MSG, SCRAP=>\@SCRAP});
}


##########################################################################
# FIXME: DEBUG COMMAND USE TO DO:: EH :: DEBUGGING :-)
sub _Command_Destroy {
	my($self,$sha1) = @_;
	my @MSG   = ();
	my @SCRAP = ();
	if($sha1 && $self->Torrent->ExistsTorrent($sha1) && (my $torrent = $self->Torrent->GetTorrent($sha1)) ) {
		$self->warn("Will destroy some random pieces of $sha1 <$torrent>");
		my $piecenum = $torrent->Storage->GetSetting('chunks');
		for(0..10) {
			my $rnd = int(rand($piecenum));
			next unless $torrent->Storage->IsSetAsDone($rnd);
			$torrent->Storage->SetAsInworkFromDone($rnd);
			$torrent->Storage->Truncate($rnd);
			$torrent->Storage->SetAsFree($rnd);
			$self->warn("Piece $rnd is gone. byebye!");
		}
		$self->cancel_torrent(Sid=>$sha1, Internal=>1);
		$self->resume_this($sha1);
	}
	push(@MSG, [1, "Did something"]);
	return({MSG=>\@MSG, SCRAP=>\@SCRAP});
}

##########################################################################
# Verify a single piece if a verify job exists
sub RunVerification {
	my($self, $reloop) = @_;
	
	foreach my $sid (keys(%{$self->{verify}})) {
		my $obj     = $self->{verify}->{$sid};
		my $piece   = $obj->{piece}++;
		my $torrent = $obj->{torrent};
		
		$self->debug("RunVerification for $piece in loop $reloop");
		
		if($piece == ($torrent->Storage->GetSetting('chunks'))) {
			$self->warn("Verification of $sid has ended");
			
			foreach my $badpiece (keys(%{$obj->{bad}})) {
				$self->warn("Invalidating bad piece $badpiece in $sid");
				$torrent->Storage->SetAsInworkFromDone($badpiece);
				$torrent->Storage->Truncate($badpiece);
				$torrent->Storage->SetAsFree($badpiece);
			}
			
			$self->cancel_torrent(Sid=>$sid, Internal=>1); # Internal=>1 makes us reloading the torrrent
			$self->resume_this($sid);
		}
		elsif( (my $xdone = $torrent->Storage->IsSetAsDone($piece)) or $torrent->Storage->IsSetAsFree($piece) ) {
			
			($xdone ? $torrent->Storage->SetAsInworkFromDone($piece) : $torrent->Storage->SetAsInwork($piece));
			
			my $xsize = $torrent->Storage->GetTotalPieceSize($piece);
			my $is_ok = $self->Peer->VerifyOk(Torrent=>$torrent, Index=>$piece, Size=>$xsize);
			
			if( $xdone ) {
				$torrent->Storage->SetAsDone($piece);        # Move piece back
				$obj->{bad}->{$piece} = defined if !$is_ok;  # but mark it as bad (for later invalidation)
			}
			else { # Piece was free: Set it as DONE if vrfy was OK ; Set it to Free otherwise 
				if($is_ok) {
					$torrent->Storage->Truncate($piece,$xsize); # Fixup size
					$torrent->Storage->SetAsDone($piece);
				}
				else {
					$torrent->Storage->SetAsFree($piece);
				}
			}
			$self->RunVerification(--$reloop) if $reloop > 1;
		}
		# Else: Inwork piece
		
		return 1;
	}
	
	$self->{verify_task} = undef; # Fixme: Could ->destroy (in SxTask) set this?
	
	return 0; # Task can stop now
}

##########################################################################
# Load / Resume a torrent file
sub resume_this {
	my($self, $sid) = @_;
	
	my $so             = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage $sid : $!");
	my $done_bytes     = 0;
	my $done_chunks    = 0;
	my $total_bytes    = (($so->GetSetting('chunks')-1) * $so->GetSetting('size')) + ($so->GetSetting('size') - $so->GetSetting('overshoot'));
	my $torrent        = undef;
	
	if(my $rdata = $so->GetSetting('_torrent')) {
		my $href = $self->{super}->Tools->BencDecode($rdata);
		$torrent = $self->Torrent->AddNewTorrent(StorageId=>$sid, Torrent=>$href);
		if($torrent->GetSha1 ne $sid) {
			$self->stop("Corrupted download directory: '$sid': Torrent-Hash (".$torrent->GetSha1.") does not match, aborting.");
		}
	}
	elsif(my $sdata = $so->GetSetting('_metahash')) {
		$torrent = $self->Torrent->AddNewTorrent(StorageId=>$sid, MetaHash=>$sdata);
	}
	else {
		$self->panic("$sid has no valid hash information: no _torrent / _metahash objects found");
	}
	
	for my $cc (1..$so->GetSetting('chunks')) {
		$cc--; # PieceCount starts at 0, but cunks at 1
		my $fullpiece_size = $torrent->GetTotalPieceSize($cc);
		
		if($so->IsSetAsDone($cc)) {
			my $this_size = $so->GetSizeOfDonePiece($cc);
			if($this_size != $fullpiece_size) {
				$self->warn("Done-Piece $cc has an invalid size, truncating piece ($this_size != $fullpiece_size)");
				$so->SetAsInworkFromDone($cc);
				$so->Truncate($cc);
				$so->SetAsFree($cc);
			}
			else {
				$torrent->SetBit($cc);
				$done_bytes += $this_size;
				$done_chunks++;
			}
		}
		elsif($so->IsSetAsFree($cc)) {
			my $this_size = $so->GetSizeOfFreePiece($cc);
			if($this_size >= $fullpiece_size) {
				$self->warn("Free-Piece $cc is too big, truncating");
				$so->SetAsInwork($cc);
				$so->Truncate($cc);
				$so->SetAsFree($cc);
			}
		}
		else {
			$self->panic("Bug! $sid lost piece $cc");
		}
		
	}
	
	$self->{super}->Queue->SetStats($sid, {total_bytes=>$total_bytes, done_bytes=>$done_bytes, uploaded_bytes=>int($so->GetSetting('_uploaded_bytes') || 0),
	                                       active_clients=>0, clients=>0, last_recv=>int($so->GetSetting('_last_recv') || 0),
	                                       speed_upload=>0, speed_download=>0,
	                                       total_chunks=>int($so->GetSetting('chunks')), done_chunks=>$done_chunks});
	$torrent->SetStatsUp(0); $torrent->SetStatsDown(0);
	$torrent->RebuildFakeBitfield; # Set initial fakebitfield
	return 1;
}

##########################################################################
# Called by QueueMGR
sub cancel_this {
	my($self, $sid) = @_;
	$self->cancel_torrent(Sid=>$sid, Internal=>0);
}


##########################################################################
# Drop a torrent
sub cancel_torrent {
	my($self, %args) = @_;
	
	my $sid      = delete($args{Sid})        or $self->panic("No sid?!");
	my $internal = delete($args{Internal});
	
	# Ok, this is a bit tricky: First get the torrent itself
	my $this_torrent = $self->Torrent->GetTorrent($sid) or $self->panic("Torrent $sid does not exist!");
	
	# And close down the TCP connection with ALL clients linked to thisone:
	foreach my $con ($this_torrent->GetPeers) {
		$self->KillClient($self->Peer->GetClient($con));
	}
	
	# .. now remove the information about this SID from this module ..
	$self->Torrent->DestroyTorrent($sid);
	# .. remove a (possible running) verify job
	delete($self->{verify}->{$sid});
	# .. and tell the queuemgr to drop it also
	$self->{super}->Queue->RemoveItem($sid) if !$internal;
}


##########################################################################
# Mainrunner


sub run {
	my($self,$NOW) = @_;
	
	my $PH                     = $self->{phunt};                                                                    # Shortcut
	my $PH_RUNTIME             = $NOW - $PH->{lastqrun};                                                            # how long this run takes..
	$PH->{credits}             = (abs(int($self->{super}->Configuration->GetValue('torrent_gcpriority'))) or 1);    # GarbageCollector priority
	$PH->{ut_metadata_credits} = 3;
	
	$self->debug("Run is working since $PH_RUNTIME seconds and has still $PH->{phi} items left");
	if($PH->{phi} != 0 && $PH_RUNTIME > DELAY_FULLRUN*2 ) {
		$self->debug("Under pressure.. speeding up things a little bit.");
		$PH->{credits} = int($PH->{credits} + $PH->{phi}/5);
	}
	
	if($PH->{phi} == 0 && $PH_RUNTIME > MIN_LASTQRUN) {
		# -> Cache empty
		
		my @a_clients     = List::Util::shuffle($self->Peer->GetClients);
		
		$PH->{lastqrun}   = $NOW;
		$PH->{phclients}  = \@a_clients;       # Shuffled client list
		$PH->{phi}        = int(@a_clients);   # Number of clients
		$PH->{havemap}    = {};                # Clear HaveFlood map
		$PH->{dqueue}     = {};                # Clear DeliverQueue
		
		for(my $i=0; ($i<8 && $i<int(@a_clients));$i++) {
			$PH->{dqueue}->{$a_clients[$i]} = 1;
		}
		
		if($PH->{fullrun} <= $NOW-(DELAY_FULLRUN)) {
			# Issue a full-run, this includes:
			#  - Save the configuration
			#  - Rebuild the HAVEMAP
			#  - Build up/download statistics per torrent
			
			my $drift  = (int($NOW-$PH->{fullrun}) or 1);                       #
			my $xycred = 1;                                                     # Add New Pers credits
			$PH->{pexmap}     = {};  # Clear PEX-Map
			$PH->{fullrun}    = $NOW;
			
			foreach my $torrent (List::Util::shuffle($self->Torrent->GetTorrents)) {
				my $tobj    = $self->Torrent->GetTorrent($torrent);
				my $so      = $self->{super}->Storage->OpenStorage($torrent) or $self->panic("Unable to open storage for $torrent");
				
				# Try to get new peers
				if($xycred-- > 0) {
					$tobj->RebuildFakeBitfield;  # Rebuild fake bitfield
					$tobj->AddNewPeers;          # Connect to new peers
				}
				
				# Save settings
				foreach my $persisten_stats qw(uploaded_bytes last_recv) {
					$so->SetSetting("_".$persisten_stats, $self->{super}->Queue->GetStats($torrent)->{$persisten_stats});
				}
				
				# cleanup ppl list
				$tobj->UpdatePPL;
				
				# update have-map
				my @a_haves = $tobj->GetHaves; $tobj->ClearHaves;  # ReBuild HaveMap
				$PH->{havemap}->{$torrent} = \@a_haves;
				
				# setstats
				my $bps_up  = $tobj->GetStatsUp/$drift;
				my $bps_dwn = $tobj->GetStatsDown/$drift;
				$tobj->SetStatsUp(0); $tobj->SetStatsDown(0);
				$self->{super}->Queue->SetStats($torrent, {speed_upload => $bps_up, speed_download => $bps_dwn });
				
				unless($tobj->InEndgameMode) {
					# -> Torrent is not in endgame mode and not completed
					my $stats  = $self->{super}->Queue->GetStats($tobj->GetSha1);
					my $todo   = $stats->{total_chunks}-$stats->{done_chunks};
					my @locked = $tobj->TorrentwideLockList;
					
					if($todo && $todo <= 100 && int(@locked) == $todo) {
						# -> Switch to endgame mode
						$tobj->EnableEndgameMode;
						$self->debug("$tobj : Switching to endgame mode");
					}
					
				}
				
			}
		}
		
		if($PH->{lastchokerun} <= $NOW-(DELAY_CHOKEROUND) && ($PH->{lastchokerun} = $NOW)) {
			my $CAM    = $PH->{chokemap}->{can_unchoke};
			my @sorted = sort { $CAM->{$b} <=> $CAM->{$a} } keys %$CAM; 
			my $CAN_UNCHOKE = (abs(int($self->{super}->Configuration->GetValue('torrent_upslots'))) or 1);
			foreach my $this_name (@sorted) {
				next unless $self->Peer->ExistsClient($this_name);
				last if --$CAN_UNCHOKE < 0;
				$self->Peer->GetClient($this_name)->WriteUnchoke if $self->Peer->GetClient($this_name)->GetChokePEER;
				delete($PH->{chokemap}->{can_choke}->{$this_name});
			}
			
			foreach my $this_name (keys(%{$PH->{chokemap}->{can_choke}})) {
				next unless $self->Peer->ExistsClient($this_name);
				next if $self->Peer->GetClient($this_name)->GetChokePEER; # Client could have choked itself via MSG_UNINTERESTED
				$self->Peer->GetClient($this_name)->WriteChoke;
			}
			
			$PH->{chokemap} = { can_choke => {}, can_unchoke => {}, optimistic => 1, seedprio=>{}}; # Clear chokemap and set optimistic-credit to 1
		}
	}
	
	while($PH->{phi} > 0 && --$PH->{credits} >= 0) {
		my $c_index = --$PH->{phi};
		my $c_sname = ${$PH->{phclients}}[$c_index];
		next unless   $self->Peer->ExistsClient($c_sname); # Client vanished
		my $c_obj    = $self->Peer->GetClient($c_sname);
		my $c_lastio = $c_obj->GetLastIO;
		my $c_status = $c_obj->GetStatus;
		
		if($c_status != STATE_IDLE && $c_status != STATE_NOMETA) {
			# Client didn't complete handshake yet.. do a fast-timeout
			if($c_lastio < ($NOW - TIMEOUT_FAST)) {
				$self->debug("<$c_obj> did not complete handshake, dropping connection");
				$self->KillClient($c_obj);
			}
		}
		else {
			my $c_sha1    = $c_obj->GetSha1 or $self->panic("<$c_obj> has no sha1 bound");
			my $c_torrent = $self->Torrent->GetTorrent($c_sha1);
			my $c_iscompl = $c_torrent->IsComplete;
			
			if($c_obj->GetLastUsefulTime < ($NOW-(TIMEOUT_UNUSED_CLIENT))) {
				$self->debug("<$c_obj> : Dropping connection with silent client");
				$self->KillClient($c_obj);
				next;
			}
			
			if( ($c_obj->{kudos}->{fail} >= MIN_HASHFAILS) && ($c_obj->{kudos}->{fail} > ($c_obj->{kudos}->{ok}/4)) ) {
				$self->warn($c_obj->XID." : Too many errors, blacklisting peer");
				$self->{super}->Network->BlacklistIp($self, $c_obj->GetRemoteIp);
				$self->KillClient($c_obj);
				next;
			}
			
			if($c_lastio < ($NOW-(TIMEOUT_NOOP))) {
				$self->debug("<$c_obj> : Sending NOOP");
				$c_obj->WritePing;
			}
			
			#####################################################
			
			if($c_status == STATE_IDLE) {
				
				foreach my $this_piece (@{$PH->{havemap}->{$c_sha1}}) {
					## $self->debug("HaveFlooding $c_obj : $this_piece");
					$c_obj->WriteHave($this_piece);
				}
				
				if(!exists($PH->{pexmap}->{$c_sha1}) && $c_obj->GetExtension('UtorrentPex')) {
					$PH->{pexmap}->{$c_sha1} = 1;
					$self->debug("Sending PEX for $c_sha1");
					$self->_AssemblePexForClient($c_obj,$c_torrent) if $c_torrent->IsPrivate == 0;
				}
				
				if($c_obj->HasUtMetaRequest && !$c_torrent->IsPrivate && $PH->{ut_metadata_credits}--) {
					$c_obj->WriteUtMetaResponse($c_obj->GetUtMetaRequest);
				}
				
				
				if(!$c_iscompl) {
					# -> Download is incomplete, start hunt-gc
					my $time_lastrq   = $c_obj->GetLastRequestTime;
					my $this_timeout  = ($c_torrent->InEndgameMode ? TIMEOUT_PIECE_FAST : TIMEOUT_PIECE_NORM);
					my $did_release   = 0;
					if($time_lastrq+$this_timeout <= $NOW) {
						foreach my $this_piece (keys(%{$c_obj->GetPieceLocks})) {
							$c_obj->ReleasePiece(Index=>$this_piece);
							$did_release = $this_piece;
							$self->warn($c_obj->XID." -> Released $did_release from slow client");
						}
						
						if($did_release) {
							$c_obj->PenaltyHunt(25);
							$c_torrent->HuntFastClientForPiece($did_release);
						}
						
					}
					
					if($c_obj->GetNextHunt < $NOW) {
						$self->debug("$c_obj : hunting");
						$c_obj->HuntPiece;
					}
				}
				else {
					my $numpieces_peer = $c_obj->GetNumberOfPieces;
					my $numpieces_me   = $self->{super}->Queue->GetStats($c_sha1)->{done_chunks};
					
					if($numpieces_me == $numpieces_peer) {
						# Disconnect from seeding peer
						$self->KillClient($c_obj);
						next;
					}
					elsif($c_obj->GetInterestedME) {
						# Completed but still interested? -> Write uninterested message
						$self->debug("<$c_obj> Not interested (we are completed)");
						$c_obj->WriteUninterested;
					}
				
				}
				
				if(!$c_obj->GetChokePEER) {
					# Peer is unchoked, we could choke it
					$PH->{chokemap}->{can_choke}->{$c_sname} = $c_sname;
				}
				
				if(!exists($PH->{chokemap}->{can_unchoke}->{$c_sname}) && $c_obj->GetInterestedPEER && !$c_torrent->IsPaused) {
					# Peer is interested and torrent is not paused -> We can unchoke it
					my $foopoints = ($c_iscompl ? $c_obj->GetAvgSentInPercent : $c_obj->GetAvgStoredInPercent);
					
					if($PH->{chokemap}->{seedprio}->{$c_sha1}++ < $c_torrent->GetSeedPriority) {
						$foopoints = 0xFFFFFFFE;
					}
					elsif(delete($PH->{chokemap}->{optimistic})) {
						$foopoints = 0xFFFFFFFF;
					}
					
					$PH->{chokemap}->{can_unchoke}->{$c_sname} = $foopoints;
				}
				
				# Deliver pieces queued pieces
				if(exists($PH->{dqueue}->{$c_sname})) {
					$self->debug("Flushing DeliverQueue of $c_sname");
					$c_obj->FlushDeliverQueue;
				}
				
				#END
			}
			elsif($c_status == STATE_NOMETA) {
				
				if($c_obj->GetExtension('UtorrentMetadataSize') && $c_obj->GetExtension('UtorrentMetadata') && !$c_obj->HasUtMetaRequest && $PH->{ut_metadata_credits}--) {
					$c_obj->WriteUtMetaRequest;
				}
				
			}
			else {
				$self->panic("In state $c_status but i shouldn't");
			}
		}
	}
	return 4;
}






##########################################################################
# Call Torrent class
sub Torrent {
	my($self, %args) = @_;
	$self->{Dispatch}->{Torrent};
}

##########################################################################
# Call Torrent class
sub Peer {
	my($self, %args) = @_;
	$self->{Dispatch}->{Peer};
}


##########################################################################
# Creates a PEX message for $torrent object and sends the
# result to $client (if we found any useable clients)
sub _AssemblePexForClient {
	my($self, $client, $torrent) = @_;
	
	my $xref  = {dropped=>'', added=>'', 'added.f'=>'', 'added6'=>'', 'added6.f'=>''};  # Hash to send
	my $pexc  = 0;                                                                      # PexCount
	my $pexid = $client->GetExtension('UtorrentPex');                                   # ID we are using to send message
	
	foreach my $cid ($torrent->GetPeers) {
		my $cobj                     = $self->Peer->GetClient($cid);
		my $remote_port              = $cobj->GetRemotePort;
		my $remote_ip                = $cobj->GetRemoteIp;
		
		next if $cobj->GetStatus     != STATE_IDLE;                               # No normal peer connection
		next if $remote_port         == 0;                                        # We don't know the remote port -> can't publish this contact
		last if ++$pexc              >= PEX_MAXPAYLOAD;                           # Maximum payload reached, stop search
		
		
		if($self->{super}->Network->IsNativeIPv6($remote_ip)) {
			my @ipv6 = $self->{super}->Network->ExpandIpV6($remote_ip);
			my $pkt = join('', map(pack("n",$_),(@ipv6,$remote_port)));
			
			$xref->{'added6'}   .= $pkt;
			$xref->{'added6.f'} .= chr( ( $cobj->GetExtension('Encryption') ? 1 : 0 ) ); # 1 if client told us that it talks silly-encrypt
			
		}
		elsif( $self->{super}->Network->IsValidIPv4($remote_ip) or ($remote_ip = $self->{super}->Network->SixToFour($remote_ip)) ) {
			map($xref->{'added'} .= pack("C",$_), split(/\./,$remote_ip));
			$xref->{'added'}     .= pack("n",$remote_port);
			$xref->{'added.f'}   .= chr( ( $cobj->GetExtension('Encryption') ? 1 : 0 ) ); # 1 if client told us that it talks silly-encrypt
		}
		
	}
	
	if($pexc && $pexid) {
		# Found some clients -> send it to the 'lucky' peer :-)
		$self->debug($client->XID." Sending $pexc pex nodes");
		$client->WriteEprotoMessage(Index=>$pexid, Payload=>$self->{super}->Tools->BencEncode($xref));
	}
	
}


##########################################################################
# Load a .torrent file from disk and init the storage
sub LoadTorrentFromDisk {
	my($self, @args) = @_;
	
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	foreach my $file (@args) {
		my $ref = $self->{super}->Tools->BencfileToHash($file);
		if(defined($ref->{content}) && exists($ref->{content}->{info})) {
				my $torrent_hash = $self->{super}->Tools->sha1_hex($self->{super}->Tools->BencEncode($ref->{content}->{info}));
				my $numpieces  = (length($ref->{content}->{info}->{pieces})/SHALEN);
				my $piecelen   = $ref->{content}->{info}->{'piece length'};
				my $filelayout = ();
				my $xtotalsize = $numpieces * $piecelen;
				my $overshoot  = undef;
				my $ccsize     = 0;
				
				if($numpieces < 1) {
					push(@MSG, [2, "file $file has no valid SHA1 string, skipping corrupted torrent"]);
					next;
				}
				if($ref->{content}->{info}->{length}) {
					$overshoot = $xtotalsize - $ref->{content}->{info}->{length};
					$filelayout->{Fark} = { start => 0, end=> $ref->{content}->{info}->{length}, path => [$ref->{content}->{info}->{name}]};
				}
				elsif(ref($ref->{content}->{info}->{files}) eq "ARRAY") {
					foreach my $cf (@{$ref->{content}->{info}->{files}}) {
						
						my $unixpath = join('/', @{$cf->{path}});
						$filelayout->{$unixpath} = { start => $ccsize, path => $cf->{path} };
						$ccsize += $cf->{length};
						$filelayout->{$unixpath}->{end}   = $ccsize;
					}
					$overshoot = $xtotalsize - $ccsize;
				}
				else {
					push(@MSG, [2, "file $file is missing size information, skipping corrupted torrent."]);
					next;
				}
				
				
				my $so = $self->{super}->Queue->AddItem(Name=>$ref->{content}->{info}->{name}, Chunks=>$numpieces, Overshoot=>$overshoot,
				                                          Size=>$piecelen, Owner=>$self, ShaName=>$torrent_hash, FileLayout=>$filelayout);
				if($so) {
					$so->SetSetting('_torrent', $ref->{raw_content})         or $self->panic("Unable to store torrent file as setting : $!");
					$so->SetSetting('type', ' bt ')                          or $self->panic("Unable to store type setting : $!");
					$self->resume_this($torrent_hash);
					push(@MSG, [1, "$torrent_hash: BitTorrent file $file loaded"]);
				}
				else {
					push(@MSG, [2, "$@"]);
				}
		}
		elsif($file =~ /^torrent:\/\/([0-9a-f]+)/i) {
			my $sha1 = lc($1);
			if(length($sha1) == SHALEN*2) {
				my $b32 = $self->{super}->Tools->encode_b32(pack("H*",$sha1));
				push(@args, "magnet:?xt=urn:btih:$b32");
			}
			else {
				push(@MSG, [2, "Invalid info-hash: $sha1"]);
			}
		}
		elsif($file =~ /^magnet:\?/) {
			my $magref   = $self->{super}->Tools->decode_magnet($file);
			my $magname = $magref->{dn}->[0]->{':'} || "$file";
			foreach my $xt (@{$magref->{xt}}) {
				my($k,$v) = each(%$xt);
				next if $k ne 'urn:btih'; # not interesting
				
				
				my $sha1 = $self->{super}->Tools->decode_b32($v);
				   $sha1 = pack("H*",$v) if length($sha1) != SHALEN && $v =~ /^([0-9a-f]{40})$/i; # some people out there forget to do b32 encoding...
				
				if(length($sha1) != SHALEN) {
					push(@MSG, [2, "Invalid info-hash: $v , skipping magnet link."]);
					next;
				}
				
				# convert (correct sized) string to hex:
				$sha1 = unpack("H*",$sha1);
				
				# create fake-storage
				my $so = $self->{super}->Queue->AddItem(Name=>"$magname", Chunks=>1, Overshoot=>0, Size=>UTMETA_MAXSIZE, Owner=>$self,
				                                        ShaName=>$sha1, FileLayout=> { foo => { start => 0, end => UTMETA_MAXSIZE, path => ["Torrent Metadata for $magname"] } });
				if($so) {
					$so->SetSetting('type', '[bt]');
					$so->SetSetting('_metahash', $sha1);
					$self->resume_this($sha1);
					push(@MSG, [1, "$sha1: Loading BitTorrent Metadata"]);
				}
				else {
					push(@MSG, [2, "$@"]);
				}
				
			}
		}
		else {
			push(@SCRAP, $file);
		}
	}
	
	if(!int(@args)) {
		$NOEXEC = 'Usage: load /path/to/file.torrent';
	}
	
	return({MSG=>\@MSG, SCRAP=>\@SCRAP, NOEXEC=>$NOEXEC});
}


##########################################################################
# Called by sxtask: replace torrent with in-memory metadata
sub SxSwapTorrent {
	my($self,$sha1) = @_;
	
	my $tref = $self->Torrent;
	
	if( $tref->ExistsTorrent($sha1) && (my $tobj = $tref->GetTorrent($sha1)) ) {
		my $swap = $tobj->GetMetaSwap or $self->panic("SxTask has no swapdata!");
		
		# this was verified by our creator, so we do not have to check for the
		# bencoded data to be ok.
		my $path = $self->{super}->Tools->GetExclusiveTempfile;
		open(S, ">", $path) or $self->panic("Cannot write to $path : $!");
		print S $swap;
		close(S);
		
		$self->{super}->Admin->ExecuteCommand('cancel',  $sha1);
		$self->{super}->Admin->ExecuteCommand('history', $sha1, 'forget');
		$self->{super}->Admin->ExecuteCommand('load',    $path);
		unlink($path) or $self->panic("Cannot remove $path : $!");
	}
	
	return 0; # destroy sxtask
}


##########################################################################
# Try to create a new connection to a peer
sub CreateNewOutgoingConnection {
	my($self,$hash,$ip,$port) = @_;
	
	if($port <= CONN_OUT_MINPORT) {
		$self->debug("Will not connect to $ip:$port (suspect port number)");
	}
	elsif(!$self->{super}->Network->HaveIPv6 && $self->{super}->Network->IsValidIPv6($ip)) {
		$self->debug("Won't connect to IPv6 peer without IPv4 networking support");
	}
	elsif((my $torrent = $self->Torrent->GetTorrent($hash) ) && $ip && $port) {
		if($torrent->IsPaused) {
			$self->debug("$hash is paused, won't create a new connection");
		}
		elsif( ( int( $self->{super}->Configuration->GetValue('torrent_maxpeers')*0.7 ) > $self->{super}->Queue->GetStats($hash)->{clients}) && 
		       (my $sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$port, RemoteIp=>$ip, Timeout=>5)) ) {
			my $client = $self->Peer->AddNewClient($sock, {Port=>$port, RemoteIp=>$ip});
			$client->SetSha1($hash);
			$client->WriteHandshake;
			$client->SetStatus(STATE_READ_HANDSHAKERES);
			
			if($client->GetConnectionCount != 1) {
				$self->debug("Dropping duplicate connection with $ip");
				$self->KillClient($client); # We didn't really connect yet btw.. (SYN was not sent)
			}
		}
		else {
			$self->debug("Connection not established for Hash=>$hash, Ip=>$ip, Port=>$port");
		}
	}
	else {
		$self->debug("Invalid call for Hash=>$hash, Ip=>$ip, Port=>$port");
	}
}


##########################################################################
# Callback : Accept new incoming connection
sub _Network_Accept {
	my($self, $sock, $ip) = @_;
	
	$self->debug("New incoming connection $ip (<$sock>)");
	my $client = $self->Peer->AddNewClient($sock, {RemoteIp => $ip, Port => 0});
	$client->SetStatus(STATE_READ_HANDSHAKE);
}

##########################################################################
# Callback : Deregister connection
sub _Network_Close {
	my($self,$sock) = @_;
	$self->debug("TCP-Connection to $sock has been lost");
	
	my $client = $self->Peer->GetClient($sock);
	
	foreach my $lock (keys(%{$client->GetPieceLocks})) {
		$client->ReleasePiece(Index=>$lock);
	}
	
	if($client->GetSha1) {
		if(!($client->GetChokeME)) {
			$self->{super}->Queue->DecrementStats($client->GetSha1, {'active_clients' => 1});
		}
		$self->{super}->Queue->DecrementStats($client->GetSha1, {'clients' => 1});
	}
	
	$client->Destroy;
}

##########################################################################
# Callback : Data to read
sub _Network_Data {
	my($self,$sock,$buffref,$blen) = @_;
	
	my $RUNIT  = 1;
	my $client = $self->Peer->GetClient($sock) or $self->panic("Cannot handle non-existing client for sock <$sock>");
	$client->AppendReadBuffer($buffref,$blen); # Append new data to client's full buffer
	# XXX: NEEDS API
	if($client->{readbuff}->{minlen} && $client->{readbuff}->{minlen} > $client->{readbuff}->{len}) {
		# $self->warn("Still waiting for more :: $sock -> $client->{readbuff}->{minlen} > $client->{readbuff}->{len}");
		return;
	}
	else {
		$client->{readbuff}->{minlen} = 0;
	}
	
	while($RUNIT == 1) {
		my $status      = $client->GetStatus;
		my($cbref,$len) = $client->GetReadBuffer;
		my $cbuff       = ${$cbref};
		
		return if $len < BTMSGLEN;
		
		if(($status == STATE_READ_HANDSHAKE or $status == STATE_READ_HANDSHAKERES) && $len >= 68) {
			$self->debug("-> Reading handshake from peer");
			my $hs       = $self->ParseHandshake($cbref,$len);
			my $metasize = 0;
			$client->DropReadBuffer(68); # Remove 68 bytes (Handshake) from buffer
			
			if(!defined($hs->{sha1}) or !$self->Torrent->ExistsTorrent($hs->{sha1})) {
				$self->debug($client->XID." failed to complete initial handshake");
				$self->KillClient($client);
				return;
			}
			elsif($hs->{peerid} eq $self->{CurrentPeerId}) {
				$self->debug($client->XID." connected to myself (same peerid), blacklisting my own IP");
				$self->{super}->Network->BlacklistIp($self, $client->GetRemoteIp);
				$self->LearnOwnIp($client->GetRemoteIp);
				$self->KillClient($client);
				return;
			}
			else {
				if($self->{super}->Queue->GetStats($hs->{sha1})->{clients} >= $self->{super}->Configuration->GetValue('torrent_maxpeers')) {
					$self->debug("<$client> $hs->{sha1} has reached torrent_maxpeers ; dropping new connection");
					$self->KillClient($client);
					return; # Go away!
				}
				else {
					$client->SetExtensions(Kademlia=>$hs->{EXT_KAD}, ExtProto=>$hs->{EXT_EPROTO});
					$client->SetRemotePeerID($hs->{peerid});
					
					if($status == STATE_READ_HANDSHAKE) {
						# This was an incoming connection
						$client->SetSha1($hs->{sha1});
						$client->WriteHandshake;
					}
					
					# Handshake done, client is now in a normal state
					my $this_torrent = $self->Torrent->GetTorrent($client->GetSha1);
					
					if($this_torrent->IsPaused) {
						$self->debug("Dropping connection for paused torrent with client ".$client->XID);
						$self->KillClient($client);
						return; # Go away
					}
					if($client->GetConnectionCount != 1) {
						# Duplicate connection
						$self->debug("Dropping duplicate, incoming connection with ".$client->XID);
						$self->KillClient($client);
						return; # Go away
					}
					
					if($metasize = $this_torrent->GetMetaSize) {
						# We got the meta of this torrent
						$client->SetBitfield(pack("B*", ("0" x length(unpack("B*",$self->Torrent->GetTorrent($client->GetSha1)->GetBitfield))))); # Fixme: This could be some smarter perl code
						$client->SetStatus(STATE_IDLE);
					}
					else {
						# Switch client to NOMETA mode
						$client->SetStatus(STATE_NOMETA);
					}
					
					if($client->GetExtension('ExtProto')) {
						$client->WriteEprotoHandshake(Port=>$self->{super}->Configuration->GetValue('torrent_port'), Version=>'Bitflu '.BUILDID, Metasize=>$metasize,
						                              UtorrentMetadata=>EP_UT_METADATA, UtorrentPex=>EP_UT_PEX);
					}
					
					if($client->GetExtension('Kademlia')) {
						$client->WriteDhtPort($self->{super}->Configuration->GetValue('torrent_port'));
					}
					
					if($client->GetStatus == STATE_IDLE) {
						# Write bitfield: normal connection!
						$client->WriteBitfield;
					}
				}
			}
		}
		else {
			my ($msglen,$msgtype) = unpack("NC",$cbuff);
			my $payloadlen = BTMSGLEN+$msglen;
			my $readAT     = BTMSGLEN+1;
			my $readLN     = $payloadlen-$readAT;
			
			if($payloadlen > $len) { # Need to wait for more data
				$client->{readbuff}->{minlen} = $payloadlen;
				return
			}
			else {
				## WARNING:: DO NEVER USE NEXT INSIDE THIS LOOP BECAUSE THIS WOULD SKIP DROPREADBUFFER
				
				if($msglen == 0) {
					$self->debug($client->XID." sent me a keepalive");
				}
				elsif($status == STATE_IDLE) {
					if($msgtype == MSG_PIECE) {
						
						my $toread = $readLN-8; # Drop N N
						my(undef,undef,$this_piece,$this_offset,$this_data) = unpack("NC NN a$toread",$cbuff);
						
						$client->SetLastUsefulTime;
						$client->StoreData(Index=>$this_piece, Offset=>$this_offset, Size=>$readLN-8, Dataref=>\$this_data) 
						&& $client->HuntPiece($this_piece); # hunt if store returned TRUE
						
						# ..and also update some bandwidth related stats:
						$self->Torrent->GetTorrent($client->GetSha1)->SetStatsDown($self->Torrent->GetTorrent($client->GetSha1)->GetStatsDown+$readLN);
					}
					elsif($msgtype == MSG_REQUEST) {
						my(undef,undef, $this_piece, $this_offset, $this_size) = unpack("NC N N N", $cbuff);
						$self->debug("PeerRequest { Index=> $this_piece , Offset => $this_offset , Size => $this_size }");
						$client->PushDeliverQueue(Index=>$this_piece, Offset=>$this_offset, Size=>$this_size);
						$client->FlushDeliverQueue; # Try to deliver 1 piece to socket
						$client->SetLastUsefulTime;
					}
					elsif($msgtype == MSG_EPROTO) {
						$self->debug("<$client> -> EPROTO");
						$client->ParseEprotoMSG(substr($cbuff,$readAT,$readLN));
					}
					elsif($msgtype == MSG_CHOKE && !$client->GetChokeME) {
						$client->SetChokeME;
						$self->{super}->Queue->DecrementStats($client->GetSha1, {'active_clients' => 1});
						$self->debug($client->XID." -> Choked me");
					}
					elsif($msgtype == MSG_UNCHOKE && $client->GetChokeME) {
						$self->{super}->Queue->IncrementStats($client->GetSha1, {'active_clients' => 1});
						$self->debug($client->XID." -> Unchoked me");
						$client->SetUnchokeME;                           # Mark myself as unchoked
						$client->HuntPiece if !$client->GetInterestedME; # We are (currently) not interested. HuntPiece may change this
						$client->HuntPiece if $client->GetInterestedME;  # (Finally) interested: -> Hunt!
					}
					elsif($msgtype == MSG_INTERESTED) {
						$client->SetInterestedPEER;
						$self->debug("<$client> -> Is interested");
					}
					elsif($msgtype == MSG_UNINTERESTED) {
						$client->SetUninterestedPEER;
						$client->WriteChoke unless $client->GetChokePEER;
						$self->debug("<$client> -> Is Not interested");
					}
					elsif($msgtype == MSG_HAVE) {
						my (undef,undef, $have_piece) = unpack("NC N",$cbuff);
						$client->SetBit($have_piece);
						$client->TriggerHunt unless $self->Torrent->GetTorrent($client->GetSha1)->GetBit($have_piece);
						## $self->debug("<$client> has piece: $have_piece");
					}
					elsif($msgtype == MSG_CANCEL) {
						$self->debug("Ignoring cancel request because we do never queue-up REQUESTs.");
					}
					elsif($msgtype == MSG_PORT) {
						my (undef,undef,$dht_port) = unpack("NC n",$cbuff);
						$client->SetRemotePort($dht_port);
						$self->debug("Got remote port: $dht_port");
					}
					elsif($msgtype == MSG_BITFIELD) {
						$self->debug("<$client> -> BITFIELD");
						$client->SetBitfield(substr($cbuff,$readAT,$readLN));
						if($client->GetNumberOfPieces > $self->Torrent->GetTorrent($client->GetSha1)->GetNumberOfPieces) {
							# client has more pieces than we have? -> hunt asap!
							$client->HuntPiece;
						}
					}
					else {
						$self->debug($client->XID." Dropped Message: TYPE:$msgtype ;; MSGLEN: $msglen ;; LEN: $payloadlen ;; => unknown type or wrong state");
					}
				}
				elsif($status == STATE_NOMETA) {
					if($msgtype == MSG_EPROTO) {
						$self->debug("<$client> -> STATE_NOMETA EPROTO");
						$client->SetLastUsefulTime;
						$client->ParseEprotoMSG(substr($cbuff,$readAT,$readLN));
					}
					$client->SetLastUsefulTime(1) unless $client->GetExtension('UtorrentMetadata'); # Ditch this client asap
				}
				else {
					#$self->panic("Invalid state: $status"); # still handshaking
				}
				# Drop the buffer
				$client->DropReadBuffer($payloadlen) || return; # nothing more to read
			}
		}
		
	}
	
	
}




##########################################################################
# Parse BitTorrent Handshake (This is in MAIN because we do not have a peer-object yet)
sub ParseHandshake {
	my($self, $dataref, $datalen) = @_;
	my $ref = {peerid => undef, sha1 => undef, EXT_KAD => 0, EXT_EPROTO => 0};
	my $buff = ${$dataref};
	$self->panic("Short handshake! $datalen too small") if $datalen < 68;
	my $header    = unpack("c",substr($buff,0,1));
	my $hid       = substr($buff,1,19);
	my $rawext    = unpack("B*", substr($buff,20,8));
	my $info_hash = substr($buff,28,20);
	my $client_id = substr($buff,48,20);
	if($header == 19 && $hid eq "BitTorrent protocol") {
		$ref->{peerid}     = $client_id;
		$ref->{sha1}       = unpack("H*",$info_hash);
		$ref->{EXT_KAD}    = (substr($rawext, 63,1) == 1 ? 1 : 0);
		$ref->{EXT_EPROTO} = (substr($rawext, 43,1) == 1 ? 1 : 0);
	}
	else {
		$self->debug("ParseHandshake: Mumbled header: ($header) :: $hid");
	}
	return $ref;
}

##########################################################################
# Ditch client
sub KillClient {
	my($self, $client) = @_;
	$self->_Network_Close($client->GetOwnSocket);
	$self->{super}->Network->RemoveSocket($self, $client->GetOwnSocket);
	return undef;
}


## FIXME: Das RAW abspeichern, ist ev. doof? sollten wir es cachen?
sub LearnOwnIp {
	my($self, $remote_ip) = @_;
	
	if( $self->{super}->Network->IsNativeIPv6($remote_ip) ) {
		my @octets = $self->{super}->Network->ExpandIpV6($remote_ip);
		$self->{ownip}->{ipv6} = join('', map(pack("n",$_),@octets));
	}
	elsif( $self->{super}->Network->IsValidIPv4($remote_ip) or ($remote_ip = $self->{super}->Network->SixToFour($remote_ip)) ) {
		$self->{ownip}->{ipv4} = join('', map(pack("C",$_), split(/\./,$remote_ip)));
	}
	
	while(my($k,$v) = each(%{$self->{ownip}})) {
		$self->debug("$k = ".unpack("H*",($v||'')));
	}
	
}



sub debug { my($self, $msg) = @_; $self->{super}->debug("BTorrent: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("BTorrent: ".$msg);  }
sub stop  { my($self, $msg) = @_; $self->{super}->stop("BTorrent: ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("BTorrent: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("BTorrent: ".$msg); }


package Bitflu::DownloadBitTorrent::Torrent;
	use strict;
	use constant SHALEN            => 20;
	use constant ALMOST_DONE       => 30;
	use constant PPSIZE            => 8;
	use constant MAX_SAVED_PEERS   => 64; # Do not save more than 64 nodes;
	use constant MAX_CONNECT_PEERS => 8;  # Try to connect to x peers per AddNewPeers run
	
	use fields qw(super _super Torrents sha1 vrfy storage_object fake endgame_mode Sockets piecelocks haves private metadata metasize metaswap bitfield bwstats ppl fast_peers);
	
	##########################################################################
	# Returns a new Dispatcher Object
	sub new {
		my($class, %args) = @_;
		
		my $self = fields::new($class);
		$self->{super} = $args{super};
		$self->{_super} = $args{_super};
		$self->{Torrents} = {};
		return $self;
	}
	
	
	sub XXX_BitfieldDump {
		my($self) = @_;
		
		my @A = ();
		foreach my $hash ($self->GetTorrents) {
			my $bf = $self->GetTorrent($hash)->GetBitfield;
			push(@A,[3, "Torrent: $hash"]);
			push(@A,[undef, unpack("B*",$bf)]);
			push(@A,[undef, '']);
		}
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	

	
	
	##########################################################################
	# Register a .torrent file
	sub AddNewTorrent {
		my($self, %args) = @_;
		my $sid      = $args{StorageId}                           or $self->panic("No StorageId");
		my $so       = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage $sid");
		my $pieces   = $so->GetSetting('chunks')                  or $self->panic("$sid has no chunks?!");
		my $sha1     = 0;
		my $metadata = undef;
		my $metasize = 0;
		my $metacomp = 0;
		my $torrent  = {};
		
		if($args{Torrent}) {
			$torrent  = $args{Torrent};
			$metadata = $self->{super}->Tools->BencEncode($torrent->{info});
			$metasize = length($metadata);
			$sha1     = $self->{super}->Tools->sha1_hex($metadata);
		}
		elsif($args{MetaHash}) {
			# We do not have any metadata, just a known
			# hash. Be careful while handling such torrents, some commands
			# may panic bitflu due to missing information (Eg: You cannot receive pieces for such a torrent)
			$sha1 = $args{MetaHash};
			$so->SetSetting('_metasize',0); # init metasize (no resume but who cares...)
			$self->panic("$so: Directory corrupted, invalid _metahash value, expected $sha1") if $so->GetSetting('_metahash') ne $sha1;
		}
		else {
			$self->panic("No Torrent and no MetaHash ?!");
		}
		
		
		$self->panic("BUGBUG: Existing torrent! $sha1") if($self->{Torrents}->{$sha1});
		my $xo_ptype = {  sha1=>$sha1, vrfy=>$torrent->{info}->{pieces}, storage_object =>$so, bitfield=>[],
		                  fake=>{bitfield=>[]}, endgame_mode=>0, bwstats=> { down=>0, up=>0 },
		                  super=>$self->{super}, _super=>$self->{_super}, Sockets=>{}, piecelocks=>{}, haves=>{}, private=>0,
		                  metadata =>$metadata, metasize=>$metasize, metaswap=>'', ppl=>[], fast_peers=>['','','','',''],
		               };
		
		my $xo = fields::new(ref($self));
		map( $xo->{$_} = delete($xo_ptype->{$_}), keys(%$xo_ptype) );
		
		$self->{Torrents}->{$sha1} = $xo;
		
		$xo->SetBitfield(pack("B*","0" x $pieces));
		$xo->SetPrivate if (defined(${$torrent}{info}->{private}) && ${$torrent}{info}->{private} != 0);
		
		
		return $xo;
	}
	
	##########################################################################
	# Destroy registered torrent
	sub DestroyTorrent {
		my($self, $sha1) = @_;
		delete($self->{Torrents}->{$sha1}) or $self->panic("Unable to destroy <$sha1> : no such torrent!");
	}
	
	##########################################################################
	# Return reference for this torrent
	sub GetTorrent {
		my($self, $sha1) = @_;
		$self->panic("Cannot return non-existing torrent '$sha1'") unless $self->ExistsTorrent($sha1);
		return $self->{Torrents}->{$sha1};
	}
	
	##########################################################################
	# Returns true if given torrent (still) exists
	sub ExistsTorrent {
		my($self, $sha1) = @_;
		$self->panic("No sha1?") unless $sha1;
		return exists($self->{Torrents}->{$sha1});
	}
	
	##########################################################################
	# Returns a list of all registered torrents
	sub GetTorrents {
		my($self) = @_;
		return keys(%{$self->{Torrents}});
	}
	
	
	sub LinkTorrentToSocket {
		my($self, $sha1, $socket) = @_;
		my $obj = $self->GetTorrent($sha1) or $self->panic("Unable to link $socket to $sha1 : no such torrent exists");
		$obj->{Sockets}->{$socket} = 1;
	}
	
	sub UnlinkTorrentToSocket {
		my($self, $sha1, $socket) = @_;
		my $obj = $self->GetTorrent($sha1) or $self->panic("Unable to unlink $socket from non existent sha1 $sha1");
		delete($obj->{Sockets}->{$socket}) or $self->panic("$socket was not linked!");
	}
	
	#####################################################################################################
	#####################################################################################################
	
	
	##########################################################################
	# Connect to a bunch of peers
	sub AddNewPeers {
		my($self, @peerlist) = @_;
		
		# Populate @newpeers with given peerlist and stored data
		my @newpeers = map("$_->{ip}\t$_->{port}", @peerlist);
		push(@newpeers, split("\n", $self->Storage->GetSetting('_btnodes') || ''));
		
		my $ccount = MAX_CONNECT_PEERS;
		while(@newpeers) {
			my($ip,$port) = split("\t", shift(@newpeers));
			$self->{_super}->CreateNewOutgoingConnection($self->GetSha1, $ip, $port);
			last if --$ccount < 1;
		}
		# Save leftovers:
		$self->Storage->SetSetting('_btnodes', join("\n", splice(@newpeers,0,MAX_SAVED_PEERS)));
	}
	
	
	sub TorrentwideLockPiece {
		my($self, $piece) = @_;
		if(++$self->{piecelocks}->{$piece} == 1) {
			# This was the first lock: lock it at storage level
			$self->Storage->SetAsInwork($piece);
		}
		else {
			$self->panic("Multilock on $piece detected");
		}
		
		return $self->TorrentwidePieceLockcount($piece);
	}
	
	sub TorrentwideReleasePiece {
		my($self, $piece) = @_;
		if(--$self->{piecelocks}->{$piece} == 0) {
			# piece did exist: free memory and free it at storage-level
			delete($self->{piecelocks}->{$piece});
			$self->Storage->SetAsFree($piece);
		}
		else {
			$self->panic("Lock-Count-Mismatch on $piece detected (lockcount: $self->{piecelocks}->{$piece})");
		}
		return $self->TorrentwidePieceLockcount($piece);
	}
	
	sub TorrentwidePieceLockcount {
		my($self,$piece) = @_;
		return ( exists($self->{piecelocks}->{$piece}) ? 1 : 0 );
	}
	
	
	# Return locked pieces
	sub TorrentwideLockList {
		my($self) = @_;
		return keys(%{$self->{piecelocks}});
	}
	
	sub Storage {
		my($self) = @_;
		return $self->{storage_object};
	}
	
	## Lazy HavePiece message mechanism
	sub SetHave {
		my($self,$piece) = @_;
		return $self->{haves}->{$piece} = $piece;
	}
	sub GetHave {
		my($self,$piece) = @_;
		return $self->{haves}->{$piece};
	}
	sub GetHaves {
		my($self) = @_;
		return keys(%{$self->{haves}});
	}
	sub ClearHaves {
		my($self) = @_;
		return delete($self->{haves});
	}
	
	sub SetStatsDown {
		my($self, $value) = @_;
		$self->{bwstats}->{down} = $value;
	}
	sub GetStatsDown {
		my($self) = @_;
		return $self->{bwstats}->{down};
	}
	sub SetStatsUp {
		my($self, $value) = @_;
		$self->{bwstats}->{up} = $value;
	}
	sub GetStatsUp {
		my($self) = @_;
		return $self->{bwstats}->{up};
	}
	
	##########################################################################
	# Return (cached) size of metadata
	sub GetMetaSize {
		my($self) = @_;
		return $self->{metasize};
	}
	
	##########################################################################
	# Returns '' if metaswap is not set, otherwise a full torrent is returned
	sub GetMetaSwap {
		my($self) = @_;
		return $self->{metaswap};
	}
	
	##########################################################################
	# Activate MetaSwap Flag
	sub SetMetaSwap {
		my($self,$arg) = @_;
		$self->{metaswap} = $arg;
	}
	
	##########################################################################
	# Returns undef if we got no metadata, a string otherwise
	sub GetMetaData {
		my($self) = @_;
		return $self->{metadata};
	}
	
	##########################################################################
	# Returns seed priority for this torrent
	sub GetSeedPriority {
		my($self) = @_;
		return abs(int($self->Storage->GetSetting('_seedpriority') || 0));
	}
	
	##########################################################################
	# Sets seed priority for this torrent
	sub SetSeedPriority {
		my($self,$val) = @_;
		return $self->Storage->SetSetting('_seedpriority',abs(int($val)));
	}
	
	##########################################################################
	# Returns a list of all connected peers for given object
	sub GetPeers {
		my($self) = @_;
		return keys(%{$self->{Sockets}});
	}

	##########################################################################
	# Controls if torrent is private
	sub SetPrivate {
		my($self) = @_;
		$self->{private} = 1;
	}
	
	##########################################################################
	# Returns TRUE if torrent has been marked as private
	sub IsPrivate {
		my($self) = @_;
		return $self->{private};
	}
	
	##########################################################################
	# Returns TRUE if given torrent is paused, zero otherwise
	sub IsPaused {
		my($self) = @_;
		return $self->{super}->Queue->IsPaused($self->GetSha1);
	}
	
	##########################################################################
	# Set bit as TRUE
	sub SetBit {
		my $self    = $_[0];
		my $bitnum  = $_[1];
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		vec($self->{bitfield}->[$bfIndex],(7-$bitnum),1)         = 1;
		vec($self->{fake}->{bitfield}->[$bfIndex],(7-$bitnum),1) = 1;
	}
	
	##########################################################################
	# Returns TRUE if bit is set, FALSE otherwise
	sub GetBit {
		my $self    = $_[0];
		my $bitnum  = $_[1];
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		return vec($self->{bitfield}->[$bfIndex], (7-$bitnum), 1);
	}
	
	##########################################################################
	# Clear given bitid
	sub ZapBitFromFakebitfield {
		my $self    = $_[0];
		my $bitnum  = $_[1];
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		return vec($self->{fake}->{bitfield}->[$bfIndex], (7-$bitnum), 1) = 0;
	}
	
	##########################################################################
	# (Re-)Sets a bitfield
	sub SetBitfield {
		my $self    = $_[0];
		my $string  = $_[1];
		for(my $i=0; $i<length($string);$i++) {
			$self->{fake}->{bitfield}->[$i] = $self->{bitfield}->[$i] = substr($string,$i,1);
		}
	}
	
	##########################################################################
	# Returns a bitfield dump
	sub GetBitfield {
		my($self) = @_;
		return join('', @{$self->{bitfield}});
	}
	
	##########################################################################
	sub GetFakeBitfield {
		my($self) = @_;
		return join('', @{$self->{fake}->{bitfield}});
	}
	
	##########################################################################
	# Resets the fake bitfield using our current hide-configuration
	sub RebuildFakeBitfield {
		my($self) = @_;
		$self->debug("Rebuilding bitfield of $self->{sha1}");
		
		$self->SetBitfield($self->GetBitfield);               # Sync fakebitfield with realone
		my $hide = $self->Storage->GetSetting('_piecehide');  # Get % we have to hide
		
		if($hide) {
			my $total_chunks = $self->Storage->GetSetting('chunks');
			my $hide_pieces  = int(($total_chunks*$hide/100)+0.5);
			
			for(my $i=0;$i<=$hide_pieces;$i++) {
				$self->ZapBitFromFakebitfield(int(rand($total_chunks)));
			}
			
		}
	}
	
	##########################################################################
	# ReturnSHA1-Sum of this hash
	sub GetSha1 {
		my($self) = @_;
		return $self->{sha1} or $self->panic("No sha1!");
	}
	
	##########################################################################
	# Returns TRUE if this torrent is completed
	sub IsComplete {
		my($self) = @_;
		my $stats = $self->{super}->Queue->GetStats($self->GetSha1);
		return($stats->{total_chunks} == $stats->{done_chunks} ? 1 : 0 );
	}
	
	##########################################################################
	# Returns TRUE if this torrent is almost finished
	sub InEndgameMode {
		my($self) = @_;
		return $self->{endgame_mode};
	}
	
	##########################################################################
	# Enables endgame mode
	sub EnableEndgameMode {
		my($self) = @_;
		$self->{endgame_mode} = 1;
	}
	
	sub GetTotalPieceSize {
		my($self, $piece) = @_;
		my $pieces = $self->Storage->GetSetting('chunks');
		my $size   = $self->Storage->GetSetting('size');
		if($pieces == (1+$piece)) {
			# -> LAST piece
			$size -= $self->Storage->GetSetting('overshoot');
		}
		return $size;
	}
	
	##########################################################################
	# Return the number of completed pieces of this torrent
	sub GetNumberOfPieces {
		my($self) = @_;
		my $numpieces_torrent = unpack("B*",$self->GetBitfield);
		   $numpieces_torrent = ($numpieces_torrent =~ tr/1//);
		return $numpieces_torrent;
	}
	
	##########################################################################
	# Update PreferredPieceList
	sub UpdatePPL {
		my($self) = @_;
		
		return if $self->IsComplete;
		my $so        = $self->Storage;
		my $numpieces = $so->GetSetting('chunks');
		my @ppl       = ();
		my $credits   = 10;
		
		# pickup some (semi random) non-zero pieces
		for(my $i=int(rand($numpieces));$i<$numpieces;$i++) {
			next unless $so->IsSetAsFree($i);
			next if     $so->GetSizeOfFreePiece($i) == 0;
			last if     $credits-- == 0;
			push(@ppl, $i);
		}
		
		$self->{ppl} = \@ppl;
	}
	
	##########################################################################
	# Return current PPL
	sub GetPPL {
		my($self) = @_;
		return @{$self->{ppl}};
	}
	
	##########################################################################
	# Add this client('s socket) to our 'fastlist'
	sub MarkClientAsFast {
		my($self,$client_ref) = @_;
		shift(@{$self->{fast_peers}});
		push(@{$self->{fast_peers}}, "".$client_ref->GetOwnSocket);
		return 0;
	}
	
	##########################################################################
	# Try to dispatch given piece to one of our 'fast' clients
	sub HuntFastClientForPiece {
		my($self, $piece) = @_;
		foreach my $c_peername (List::Util::shuffle(@{$self->{fast_peers}})) {
			if($self->{_super}->Peer->ExistsClient($c_peername)) {
				my $c_obj = $self->{_super}->Peer->GetClient($c_peername);
				next if $c_obj->GetStatus != Bitflu::DownloadBitTorrent::STATE_IDLE; # only use if fully connected (very unlikely to be false [if perl reused the socket id])
				next if !$c_obj->GetBit($piece);              # Client doesn't have this piece
				next if $c_obj->GetChokeME;                   # Client choked us
				next if int(keys(%{$c_obj->GetPieceLocks}));  # Client already has requests
				
				$self->warn($c_obj->XID." is a candidate for $piece");
				$c_obj->HuntPiece($piece);
				last;
			}
		}
		return 0;
	}
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
	
1;


####################################################################################################################################################
package Bitflu::DownloadBitTorrent::Peer;
	use strict;
	
	use constant PEER_DEBUG         => 0; # remove ->debug calls at compile time
	
	use constant MSG_CHOKE          => 0;
	use constant MSG_UNCHOKE        => 1;
	use constant MSG_INTERESTED     => 2;
	use constant MSG_UNINTERESTED   => 3;
	use constant MSG_HAVE           => 4;
	use constant MSG_BITFIELD       => 5;
	use constant MSG_REQUEST        => 6;
	use constant MSG_PIECE          => 7;
	use constant MSG_CANCEL         => 8;
	use constant MSG_PORT           => 9;
	use constant MSG_EPROTO         => 20;
	

	use constant PEX_MAXACCEPT      => 30;     # Only accept 30 connections per pex message
	
	use constant PIECESIZE                => (2**14);
	use constant MAX_OUTSTANDING_REQUESTS => 16; # Upper for outstanding requests
	use constant MIN_OUTSTANDING_REQUESTS => 1;
	use constant DEF_OUTSTANDING_REQUESTS => 3;  # Default we are assuming
	use constant SHALEN                   => 20;
	
	use constant STATE_READ_HANDSHAKE    => Bitflu::DownloadBitTorrent::STATE_READ_HANDSHAKE;
	use constant STATE_READ_HANDSHAKERES => Bitflu::DownloadBitTorrent::STATE_READ_HANDSHAKERES;
	use constant STATE_IDLE              => Bitflu::DownloadBitTorrent::STATE_IDLE;
	use constant STATE_NOMETA            => Bitflu::DownloadBitTorrent::STATE_NOMETA;
	
	use constant EP_HANDSHAKE       => Bitflu::DownloadBitTorrent::EP_HANDSHAKE;
	use constant EP_UT_PEX          => Bitflu::DownloadBitTorrent::EP_UT_PEX;
	use constant EP_UT_METADATA     => Bitflu::DownloadBitTorrent::EP_UT_METADATA;
	# msg_type's for UT_METADATA:
	use constant UTMETA_REQUEST     => 0;
	use constant UTMETA_DATA        => 1;
	use constant UTMETA_REJECT      => 2;
	use constant UTMETA_MAXQUEUE    => 5;      # Do not queue up more than 5 request for metadata per peer
	use constant UTMETA_CHUNKSIZE   => 16384;  # For some reason, bittorren.org tells us that this is 64KiB, but it's 16KiB
	
	use constant HUNT_DELAY         => 136;     # How often the 'gc' should hunt
	
	use fields qw( super _super Sockets IPlist socket main remote_peerid remote_ip remote_port next_hunt sha1
	               ME_interested PEER_interested ME_choked PEER_choked rqslots bitfield rqmap rqcache time_lastuseful
	               time_lastrq kudos extensions readbuff utmeta_rq deliverq status);
	
	##########################################################################
	# Register new dispatcher
	sub new {
		my($class, %args) = @_;
		my $ptype = { super=>$args{super}, _super=>$args{_super}, Sockets => {}, IPlist => {} };
		
		my $self = fields::new($class);
		map( $self->{$_} = delete($ptype->{$_}), keys(%$ptype) );
		
		$self->{super}->Admin->RegisterCommand('peerlist',  $self, 'Command_Dump_Peers', "Display all connected peers");
		$self->{super}->Admin->RegisterCommand('clientstats', $self, 'Command_Client_Stats', "Display client breakdown");
		return $self;
	}
	
	
	sub Command_Client_Stats {
		my($self) = @_;
		my @A = ( [1, "Client statistics:"] );
		
		my $brandlist  = {};
		foreach my $sock (keys(%{$self->{Sockets}})) {
			my $sref  = $self->{Sockets}->{$sock};
			if($sref->GetRemoteImplementation =~ /^(\S+)/) {
				$brandlist->{$1}++;
			}
		}
		
		my $sorted = {};
		map( push( @{$sorted->{$brandlist->{$_}}}, $_ ), keys(%$brandlist) );
		
		foreach my $num (sort({$b<=>$a} keys(%$sorted))) {
			foreach my $cbrand (@{$sorted->{$num}}) {
				push(@A, [undef, sprintf(" %5d : %s", $num, $cbrand) ]);
			}
		}
		
		return ({MSG=>\@A, SCRAP=>[]});
	}
	
	
	sub Command_Dump_Peers {
		my($self, @args) = @_;
		
		my $filter = ($args[0] || '');
		
		my @A = ();
		push(@A, [undef, sprintf("  %-20s | %-20s | %-40s | ciCI | pieces | state |lastused| C,U,S,F,O | pUP | pDWN| rqmap", 'peerID', 'IP', 'Hash')]);
		
		my $peer_unchoked = 0;
		my $me_unchoked   = 0;
		my $rq_queue      = 0;
		foreach my $sock (keys(%{$self->{Sockets}})) {
			my $sref  = $self->{Sockets}->{$sock};
			
			my $numpieces  = $sref->GetNumberOfPieces;
			my $rqm        = join(';', keys(%{$sref->GetPieceLocks}));
			my $sha1       = ($sref->{sha1} || ''); # Cannot use GetSha1 because it panics if there is none set
			my $inout      = ($self->{super}->Network->IsIncoming($sock) ? '>' : '<');
			
			next if ($sha1 !~ /$filter/);
			
			$peer_unchoked++ unless $sref->GetChokePEER;
			$me_unchoked++   unless $sref->GetChokeME;
			$rq_queue += int(keys(%{$sref->GetPieceLocks}));
			
			my $ku = $sref->{kudos};
			my $ku_up   = sprintf("%3d",int($sref->GetAvgSentInPercent));
			my $ku_dwn  = sprintf("%3d",int($sref->GetAvgStoredInPercent));
			push(@A, [ ($sref->GetChokePEER ? undef : 1 ) ,
			  sprintf("%s %-20s | %-20s | %-40s | %s%s%s%s | %6d | %5d | %6d | $ku->{choke},$ku->{unchoke},$ku->{store},$ku->{fail},$ku->{ok} | $ku_up | $ku_dwn | %s",
				$inout,
				$self->{Sockets}->{$sock}->GetRemoteImplementation,
				$self->{Sockets}->{$sock}->GetRemoteIp,
				($sha1 || ("?" x (SHALEN*2)) ),
				$sref->GetChokeME,
				$sref->GetInterestedME,
				$sref->GetChokePEER,
				$sref->GetInterestedPEER,
				$numpieces,
				$sref->GetStatus,
				($self->{super}->Network->GetTime - $self->{Sockets}->{$sock}->GetLastUsefulTime),
				 $rqm,
				 )]);
		}
		push(@A, [4, "Uploading to     $peer_unchoked peer(s)"]);
		push(@A, [4, "Downloading from $me_unchoked peer(s)"]);
		push(@A, [4, "Queued Requests: $rq_queue"]);
		return({MSG=>\@A, SCRAP=>[]});
	}
	
	##########################################################################
	# Register a new TCP client
	sub AddNewClient {
		my($self, $socket, $args) = @_;
		$self->panic("BUGBUG: Duplicate socket: <$socket>") if exists($self->{Sockets}->{$socket});
		
		my $this_ip = $args->{RemoteIp} or $self->panic("No RemoteIP specified?");
		
		if($self->{super}->Network->IsNativeIPv6($this_ip)) {
			$this_ip = $self->{super}->Network->ExpandIpV6($this_ip);
		}
		
		my $xo_ptype = { socket=>$socket, main=>$self, super=>$self->{super}, _super=>$self->{_super}, status=>STATE_READ_HANDSHAKE,
		                 remote_peerid => '', remote_ip => $this_ip, remote_port => 0, next_hunt => 0, sha1 => '',
		                 ME_interested => 0, PEER_interested => 0, ME_choked => 1, PEER_choked => 1, rqslots => 0,
		                 bitfield => [], rqmap => {}, rqcache => {}, time_lastuseful => 0 , time_lastrq => 0,
		                 kudos => { born => $self->{super}->Network->GetTime, bytes_stored => 0, bytes_sent => 0, choke => 0, unchoke =>0, store=>0, fail=>0, ok=>0 },
		                 extensions=>{}, readbuff => { buff => '', len => 0, minlen=>0 }, utmeta_rq => [], deliverq => [] };
		
		my $xo = fields::new(ref($self));
		map( $xo->{$_} = delete($xo_ptype->{$_}), keys(%$xo_ptype) );
		
		$xo->SetRequestSlots(DEF_OUTSTANDING_REQUESTS);  # Inits slot counter to smalles possible value
		$xo->SetRemotePort($args->{Port});               #
		$xo->SetLastUsefulTime;                          # Init Useful Timer
		$self->{Sockets}->{$socket} = $xo;
		return $xo;
	}
	
	
	##########################################################################
	# Get a new, existing TCP client
	sub GetClient {
		my($self,$socket) = @_;
		my $obj = $self->{Sockets}->{$socket} or $self->panic("Unable to GetClient $socket: does not exist!");
		return $obj;
	}
	
	sub ExistsClient {
		my($self,$socket) = @_;
		return exists($self->{Sockets}->{$socket});
	}
	
	sub GetClients {
		my($self) = @_;
		return keys(%{$self->{Sockets}});
	}
	
	
	## Client subs ##
	
	sub XID {
		my($self) = @_;
		my $xpd = $self->GetRemotePeerID;
		   $xpd =~ tr/a-zA-Z0-9_-//cd;
		return "<$xpd|".$self->GetRemoteIp.":$self->{remote_port}\@$self->{sha1}>";
	}
	
	sub SetRemotePort {
		my($self,$port) = @_;
		return $self->{remote_port} = int($port || 0);
	}
	
	
	# WE got unchoked
	sub SetUnchokeME {
		my($self) = @_;
		$self->{kudos}->{unchoke}++;
		return $self->{ME_choked} = 0;
	}
	
	# WE got choked
	sub SetChokeME {
		my($self) = @_;
		$self->{kudos}->{choke}++;
		return $self->{ME_choked} = 1;
	}
	
	# we unchoked the peer
	sub SetUnchokePEER {
		my($self) = @_;
		return $self->{PEER_choked} = 0;
	}
	
	# we choked the peer
	sub SetChokePEER {
		my($self) = @_;
		return $self->{PEER_choked} = 1;
	}
	
	# WE are interested
	sub SetInterestedME {
		my($self) = @_;
		return $self->{ME_interested} = 1;
	}
	
	# WE are not interested
	sub SetUninterestedME { # peer interested in ME
		my($self) = @_;
		return $self->{ME_interested} = 0;
	}
	
	# peer is interested
	sub SetInterestedPEER { # Interested IN peer
		my($self) = @_;
		return $self->{PEER_interested} = 1;
	}
	
	# peer is not interested
	sub SetUninterestedPEER {
		my($self) = @_;
		return $self->{PEER_interested} = 0;
	}
	
	
	sub GetRemotePort {
		my($self,$port) = @_;
		return $self->{remote_port};
	}
	
	sub GetInterestedME {
		my($self) = @_;
		return $self->{ME_interested};
	}
	sub GetInterestedPEER {
		my($self) = @_;
		return $self->{PEER_interested};
	}
	sub GetChokePEER  {
		my($self) = @_;
		return $self->{PEER_choked};
	}
	sub GetChokeME {
		my($self) = @_;
		return $self->{ME_choked};
	}
	
	sub GetOwnSocket {
		my($self) = @_;
		return $self->{socket};
	}
	
	sub GetConnectionCount {
		my($self) = @_;
		my $sha1 = $self->GetSha1 or $self->panic("No sha1 for $self (".$self->GetRemoteIp.")");
		return $self->{main}->{IPlist}->{$self->GetRemoteIp}->{$sha1};
	}
	
	sub GetNextHunt {
		my($self) = @_;
		return $self->{next_hunt};
	}
	
	sub TriggerHunt {
		my($self) = @_;
		$self->{next_hunt} = 0;
	}
	
	sub PenaltyHunt {
		my($self,$amount) = @_;
		$amount ||= 300;
		
		my $old_value = $self->{next_hunt};
		my $new_value = $self->{super}->Network->GetTime+$amount;
		
		$self->{next_hunt} = $new_value if $new_value > $old_value;
		return $self->{next_hunt};
	}
	
	sub GetLastIO {
		my($self) = @_;
		return $self->{super}->Network->GetLastIO($self->GetOwnSocket);
	}
	
	############################################################
	# Get last timestamp used to mark client as 'useful'
	sub GetLastUsefulTime {
		my($self) = @_;
		return $self->{time_lastuseful};
	}
	
	############################################################
	# Set client as 'was usefull NOW'
	sub SetLastUsefulTime {
		my($self,$forced_time) = @_;
		$self->{time_lastuseful} = $forced_time || $self->{super}->Network->GetTime;
	}
	
	############################################################
	# Refresh request grace time
	sub SetLastRequestTime {
		my($self,$forced_time) = @_;
		$self->{time_lastrq} = $forced_time || $self->{super}->Network->GetTime;
	}
	
	############################################################
	# Return time of last request done by HuntPiece
	sub GetLastRequestTime {
		my($self) = @_;
		return $self->{time_lastrq};
	}

	
	sub GetRequestSlots {
		my($self) = @_;
		return $self->{rqslots};
	}
	
	sub SetRequestSlots {
		my($self,$slots) = @_;
		my $maxrq = abs(int($self->{super}->Configuration->GetValue('torrent_maxreq')) || MAX_OUTSTANDING_REQUESTS);
		
		if($slots    < MIN_OUTSTANDING_REQUESTS) { $slots = MIN_OUTSTANDING_REQUESTS; }
		elsif($slots > $maxrq                  ) { $slots = $maxrq;                   }
		return $self->{rqslots} = $slots;
	}
	
	##########################################################################
	# Add an item to deliver queue, returns FALSE if the client reached
	# it's limit
	sub PushDeliverQueue {
		my($self, %args) = @_;
		my $items = push(@{$self->{deliverq}}, {Index=>$args{Index}, Offset=>$args{Offset}, Size=>$args{Size}});
		
		if($items > MAX_OUTSTANDING_REQUESTS) {
			$self->debug($self->XID." reached queue limit, dropping last request");
			shift(@{$self->{deliverq}});
			return 0;
		}
		else {
			return 1;
		}
	}
	
	##########################################################################
	# Try to send all queued items
	sub FlushDeliverQueue {
		my($self) = @_;
		
		my @dcpy  = @{$self->{deliverq}};
		foreach my $d_ref (@dcpy) {
			my $qfree = $self->{super}->Network->GetQueueFree($self->GetOwnSocket);
			my $qlim  = $d_ref->{Size}*2;
			
			if($qfree >= $qlim) {
				shift(@{$self->{deliverq}});
				$self->DeliverData(%$d_ref);
			}
			else {
				$self->debug("Socket of ".$self->XID." is full, queueing piece request...");
				last;
			}
		}
	}
	
	##########################################################################
	# Register a metadata request
	sub AddUtMetaRequest {
		my($self, $piece) = @_;
		if($self->HasUtMetaRequest <= UTMETA_MAXQUEUE) {
			$self->debug($self->XID." UTMETA: Queueing request for $piece");
			push(@{$self->{utmeta_rq}},int($piece));
		}
		else {
			$self->warn($self->XID." UTMETA: Silently dropping request for $piece (Flooding me? No Thanks!)");
		}
	}
	
	##########################################################################
	# Returns the number of registered requests, 0 if nothing is requested
	sub HasUtMetaRequest {
		my($self) = @_;
		return int(@{$self->{utmeta_rq}});
	}
	
	##########################################################################
	# Returns the FIRST registered request and REMOVES it from the queue
	sub GetUtMetaRequest {
		my($self, $piece) = @_;
		return shift @{$self->{utmeta_rq}};
	}
	
	
	sub LockPiece {
		my($self, %args) = @_;
		$self->{_super}->Torrent->GetTorrent($self->GetSha1)->TorrentwideLockPiece($args{Index});
		$self->panic("Duplicate lock : $args{Index}") if exists($self->{rqmap}->{$args{Index}});
		$self->{rqmap}->{$args{Index}} = \%args;
	}
	
	sub ReleasePiece {
		my($self, %args) = @_;
		$self->{_super}->Torrent->GetTorrent($self->GetSha1)->TorrentwideReleasePiece($args{Index});
		delete($self->{rqmap}->{$args{Index}}) or $self->panic("$args{Index} was not Memory-Locked");
	}
	
	sub GetPieceLocks {
		my($self) = @_;
		return $self->{rqmap};
	}
	
	##########################################################################
	# Retun Piececache reference
	sub GetRequestCache {
		my($self) = @_;
		return $self->{rqcache};
	}
	
	##########################################################################
	# Puts new entries into RqCache
	sub RefillRequestCache {
		my($self,$max,@suggested) = @_;
		
		my $torrent    = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		my $num_pieces = $torrent->Storage->GetSetting('chunks');
		my @ppl        = $torrent->GetPPL;
		my $first_sugg = undef;
		
		# Autosugest near pieces
		if(scalar(@suggested)) {
			$first_sugg = $suggested[0];
			push(@ppl, $first_sugg+1) if (($num_pieces-2) >= $first_sugg);
			push(@ppl, $first_sugg-1) if $first_sugg > 0;                   # add one below if we didn't request piece 0
		}
		
		push(@suggested,@ppl);
		
		my $rqmap        = $self->GetPieceLocks;     # per client locks
		my $rqcache      = $self->GetRequestCache;   # found pieces
		my $found_pieces = scalar(keys(%$rqcache));  # found pieces count
		
		foreach my $t qw(sugg fast slow) {
			if($found_pieces >= $max)    { last;                                                  } # Got enough pieces
			elsif($t eq 'sugg')          {                                                        } # void -> work on provided list
			elsif($t eq 'fast')          { @suggested = map(int(rand($num_pieces)), (1..6));      } # Add random pieces
			elsif($t eq 'slow')          { @suggested = List::Util::shuffle(0..($num_pieces-1));  } # add all pieces (fixme: brauchen wir den shuffel? ist er schnell genug?)
			
			while($found_pieces < $max) {
				my $piece = shift(@suggested);
				
				last unless defined $piece; # hit end
				
				$self->panic("Piece out of bounds: $piece > $num_pieces-1") if $piece > ($num_pieces-1);
				
				next if exists($rqcache->{$piece});                  # exists in request cache
				next if exists($rqmap->{$piece});                    # currently downloading thisone (from THIS client)
				next if $torrent->GetBit($piece);                    # We got this
				next if !$self->GetBit($piece);                      # Client does not have this piece
				next if $torrent->Storage->IsSetAsExcluded($piece);  # Piece is excluded -> no need to request
				# -> Add piece to request cache!
				$rqcache->{$piece} = 1;
				$found_pieces++;
			}
			
			if($t eq 'sugg' && defined($first_sugg) && exists($rqcache->{$first_sugg})) {
				$self->debug("Ending hunt: found suggested piece");
				goto HUNT_FAST_SUGGESTION_END;
			}
		}
		
		if( $found_pieces < $max ) {
			$self->PenaltyHunt(HUNT_DELAY*3);
			$self->debug($self->XID." issued penalty (could not refill cache)");
		}
		
		# sorry for the goto but it makes the code much
		# more readable :-)
		HUNT_FAST_SUGGESTION_END:
		return $rqcache;
	}
	
	
	##########################################################################
	# Hunt (and request) pieces from client
	sub HuntPiece {
		my($self, @suggested) = @_;
		
		$self->PenaltyHunt(HUNT_DELAY); # tell gc to not re-call us too soon
		my $torrent = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		
		return if $torrent->IsComplete;          # Do not hunt complete torrents
		return if $torrent->IsPaused;            # Do not hunt paused torrents
		return if $self->GetNumberOfPieces == 0; # Do not hunt empty clients
		
		my $piece_locks  = scalar(keys(%{$self->GetPieceLocks}));                                 # number of locked pieces (by this client)
		my $client_can_q = $self->GetRequestSlots;                                                # Max number of request slots
		my $client_do_q  = ( $torrent->InEndgameMode && $client_can_q >= 1 ? 1 : $client_can_q);  # Max queue size we will do
		my $av_slots     = ( $client_do_q - $piece_locks );                                       # Slots we can use
		   $av_slots     = 0 if $av_slots < 0;
		   $av_slots     = 1 if $av_slots >= 1 && scalar(@suggested) == 0;                        # No suggestion? -> Slow start!
		my $rqc          = $self->RefillRequestCache($av_slots,@suggested);                       # Refill (and get) Request Cache
		my $rqn          = scalar(keys(%$rqc));                                                   # Entries in rqcache
		
		if($rqn) {
			# -> Interesting stuff
			if($self->GetInterestedME) { # We sent an INTERESTED message
				if(!$self->GetChokeME) { # ..and we are unchoked!
					
					my $could_rq = 0;
					foreach my $piece (keys(%$rqc)) {
						delete($rqc->{$piece});
						next if $torrent->GetBit($piece);                    # Got this from someone else
						next if $torrent->TorrentwidePieceLockcount($piece); # Currently locked
						last if $av_slots-- == 0;
						
						my $this_offset = $torrent->Storage->GetSizeOfFreePiece($piece);
						my $this_size   = $torrent->GetTotalPieceSize($piece);
						my $bytes_left  = $this_size - $this_offset;
						   $bytes_left  = PIECESIZE if $bytes_left > PIECESIZE;
						$could_rq++;
						$self->WriteRequest(Index=>$piece, Size=>$bytes_left, Offset=>$this_offset);
					}
					
					if($av_slots && $could_rq == 0 && $piece_locks == 0 && int(@suggested)) {
						$self->TriggerHunt; # Client would have more data but no free locks -> trigger hunt soon
						if($torrent->InEndgameMode && ($self->{super}->Network->GetTime - $self->GetLastRequestTime) < Bitflu::DownloadBitTorrent::TIMEOUT_PIECE_FAST) {
							$torrent->MarkClientAsFast($self);
						}
					}
				
				}
				else {
					$self->debug($self->XID." interested but still choked.");
				}
			}
			else {
				$self->WriteInterested;
			}
		}
		elsif( $piece_locks ) {
			# Nothing new to download but still waiting for data...
			$self->debug($self->XID." Waiting for data to complete");
		}
		elsif($self->GetInterestedME && !$torrent->InEndgameMode) {
			$self->WriteUninterested; # no longer iteresting -> nothing to download
		}
		
#		$self->warn("We could request $rqn pieces from this peer ($client_can_q ; $av_slots)");
		
	}
	
	

	
	sub DeliverData {
		my($self, %args) = @_;
		
		my $torrent      = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		
		if($self->GetChokePEER) {
			$self->debug($self->XID." Choked peer asked for data, ignoring (protocol race condition)");
		}
		elsif($torrent->GetBit($args{Index}) &&
		   $torrent->Storage->GetSizeOfDonePiece($args{Index}) >= ($args{Offset}+$args{Size})) {
			my $data     = $torrent->Storage->ReadDoneData(Offset=>$args{Offset}, Length=>$args{Size}, Chunk=>$args{Index});
			my $data_len = length($data);
			$self->panic("Short READ") if $data_len != $args{Size};
			$args{Dataref} = \$data;
			
			# Update some statistics
			$self->{super}->Queue->IncrementStats($self->GetSha1, {'uploaded_bytes' => $data_len});
			$torrent->SetStatsUp($torrent->GetStatsUp + $data_len);
			
			$self->WritePiece(%args) or $self->panic("Write failed"); # Should not fail because caller has to check qlength
		}
		else {
			$self->info($self->XID." Asked me for unadvertised data! (Index=>$args{Index})");
			$self->{kudos}->{fail}++; # this should never happen
		}
		return undef;
	}
	
	
	
	
	sub StoreData {
		my($self, %args) = @_;
		
		my $torrent             = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		my $piece_fullsize      = $torrent->GetTotalPieceSize($args{Index});
		my $piece_verified      = undef;
		my $do_store            = 0;
		my $want_more           = 0;
		my $orq                 = $self->{rqmap}->{$args{Index}};
		
		if( ($args{Offset}+$args{Size}) > $piece_fullsize ) {
			$self->warn("[StoreData] Data for piece $args{Index} would overflow! Ignoring data from ".$self->XID);
		}
		elsif(!defined($orq)) {
			# So the peer sent us data, that we did not request? (or maybe got hit by a timeout)
			if($torrent->InEndgameMode) {
				if($torrent->Storage->IsSetAsFree($args{Index}) && $args{Offset} == $torrent->Storage->GetSizeOfFreePiece($args{Index}) ) {
					$self->debug("[StoreData] Using free piece $args{Index} to store unrequested data from ".$self->XID);
					$self->LockPiece(%args); # Lock this
					$do_store = 1;           # Store this piece
				}
				elsif($torrent->TorrentwidePieceLockcount($args{Index}) && $args{Offset} == $torrent->Storage->GetSizeOfInworkPiece($args{Index})) {
					# Piece is locked -> Steal the lock
					$self->debug("[StoreData] ".$self->XID." does a STEAL-LOCK write for $args{Index}");
					
					foreach my $c_peernam ($torrent->GetPeers) {
						my $c_peerobj = $self->{_super}->Peer->GetClient($c_peernam);
						if($c_peerobj->GetPieceLocks->{$args{Index}}) {
							$c_peerobj->ReleasePiece(Index=>$args{Index});
							last;
						}
					}
					$self->LockPiece(%args);  # We just stole a lock.
					$do_store = 1;            # ..store it
				}
			}
		}
		elsif($orq->{Index} == $args{Index}  && $orq->{Size} == $args{Size} && $orq->{Offset} == $args{Offset} &&
		      $torrent->Storage->GetSizeOfInworkPiece($args{Index}) == $orq->{Offset}) {
			$self->debug("[StoreData] ".$self->XID." storing requested data") if PEER_DEBUG;
			$do_store  = 1; # Store data
			$want_more = 1; # Request rehunt
		}
		else {
			$self->warn("[StoreData] ".$self->XID." unexpected data: $orq->{Index}  == $args{Index}  && $orq->{Size} == $args{Size} && $orq->{Offset} == $args{Offset}");
		}
		
		
		
		if($do_store) {
			my $piece_nowsize  = $torrent->Storage->WriteData(Chunk=>$args{Index}, Offset=>$args{Offset}, Length=>$args{Size}, Data=>$args{Dataref});
			$self->{kudos}->{store}++;
			$self->{kudos}->{bytes_stored} += $args{Size};
			
			$self->ReleasePiece(Index=>$args{Index});
			
			if($piece_fullsize == $piece_nowsize) {
				# Piece is completed: HashCheck it
				$torrent->Storage->SetAsInwork($args{Index});
				if(!$self->VerifyOk(Torrent=>$torrent, Index=>$args{Index}, Size=>$piece_nowsize)) {
					$self->warn("Verification of $args{Index}\@".$self->GetSha1." failed, starting ROLLBACK");
					$self->warn("Peer that sent me the last piece-chunk was: ".$self->XID." (might be innocent)");
					$torrent->Storage->Truncate($args{Index});
					$torrent->Storage->SetAsFree($args{Index});
					$self->{kudos}->{fail}++;
					$want_more = 0;
				}
				elsif($piece_nowsize != $piece_fullsize) {
					$self->panic("$args{Index} grew too much! $piece_nowsize != $piece_fullsize");
				}
				else {
					$self->debug("Verification of $args{Index}\@".$self->GetSha1." OK: Committing piece.");
					$torrent->SetBit($args{Index});
					$torrent->SetHave($args{Index});
					$torrent->Storage->SetAsDone($args{Index});
					$self->{kudos}->{ok}++;
					my $qstats = $self->{super}->Queue->GetStats($self->GetSha1);
					$self->{super}->Queue->SetStats($self->GetSha1, {done_bytes => $qstats->{done_bytes}+$piece_fullsize,
					                                                 done_chunks=>1+$qstats->{done_chunks}, last_recv=>$self->{super}->Network->GetTime});
				}
			}
		}
		
		return $want_more;
	}
	
	##########################################################################
	# Returns 1 if piece is verified
	sub VerifyOk {
		my($self, %args) = @_;
		my $torrent   = $args{Torrent};
		my $piece     = $args{Index};
		my $this_size = $args{Size};
		my $sha1_file = $self->{super}->Tools->sha1($torrent->Storage->ReadInworkData(Chunk=>$piece, Offset=>0, Length=>$this_size));
		my $sha1_trnt = substr($torrent->{vrfy}, ($piece*SHALEN), SHALEN);
		return 1 if $sha1_file eq $sha1_trnt;
		return 0;
	}
	
	##########################################################################
	# Stores an UtorrentMetadata piece
	sub StoreUtMetaData {
		my($self, $bencoded) = @_;
		my $decoded         = $self->{super}->Tools->BencDecode($bencoded);
		my $client_torrent  = $self->{_super}->Torrent->GetTorrent($self->GetSha1);         # Client's torrent object
		my $client_sobj     = $client_torrent->Storage;                                     # Client's storage object
		my $metasize        = $client_sobj->GetSetting('_metasize');                        # Currently set metasize of torrent
		my $this_offset     = $decoded->{piece}*UTMETA_CHUNKSIZE;                           # We should be at this offset to store data
		my $this_bprefix    = length($self->{super}->Tools->BencEncode($decoded));          # Data begins at this offset
		my $this_payload    = substr($bencoded,$this_bprefix);                              # Payload
		my $this_payloadlen = length($this_payload);                                        # Length of payload
		my $this_psize      = $client_torrent->Storage->GetSizeOfFreePiece(0);              # current progress
		my $this_asize      = $this_psize+$this_payloadlen;                                 # 'after' progress
		my $max_storesize   = $self->{super}->Queue->GetStat($self->GetSha1,'total_bytes'); # reserved storage space
		
		if($this_psize == 0 && $metasize == 0 && $decoded->{piece} == 0) {
			# The first piece triggers _metasize (We do not care 'bout the handshake)
			$metasize = int(abs($decoded->{total_size}));
			
			if($metasize && $metasize <= $max_storesize) {
				$client_sobj->SetSetting('_metasize', $metasize);
				$self->info("metadata: metasize of ".$self->GetSha1." is $metasize bytes");
			}
			else {
				$self->warn($self->XID." reported a very unlikely metasize of $metasize bytes. Ignoring data"); 
				return; # go away dude!
			}
		}
		
		if( $metasize > $this_psize && $this_offset == $this_psize && ( $this_asize <= $metasize) &&   # not finished, correct pice and does not overflow
		    ( $this_asize == $metasize or $this_payloadlen == UTMETA_CHUNKSIZE ) ) {                   # correct size
			
			# Write data
			$client_torrent->Storage->SetAsInwork(0);
			$client_torrent->Storage->WriteData(Chunk=>0, Offset=>$this_psize, Length=>$this_payloadlen, Data=>\$this_payload);
			$client_torrent->Storage->SetAsFree(0);
			$this_psize = $this_asize; # fixup piecesize
			
			if($this_psize == $metasize) { # finished -> check if hash matches
				$client_torrent->Storage->SetAsInwork(0);
				my $raw_torrent = $client_torrent->Storage->ReadInworkData(Chunk=>0, Offset=>0, Length=>$metasize);
				my $raw_sha1    = $self->{super}->Tools->sha1_hex($raw_torrent);
				$client_torrent->Storage->SetAsFree(0);
				
				if($raw_sha1 eq $self->GetSha1) {
					my $ref_torrent = $self->{super}->Tools->BencDecode($raw_torrent);
					my $ok_torrent  = $self->{super}->Tools->BencEncode({comment=>'Downloaded via ut_metadata using Bitflu', info=>$ref_torrent});
					$client_torrent->SetMetaSwap($ok_torrent);
					$self->{super}->Admin->SendNotify($self->GetSha1.": Metadata received - loading torrent");
					$self->{super}->CreateSxTask(Args=>[$self->GetSha1], Interval=>0, Superclass=>$self->{_super}, Callback=>'SxSwapTorrent');
				}
				else {
					$self->warn($self->GetSha1.": Received torrent has an invalid hash ($raw_sha1), starting a retry...");
					$client_torrent->Storage->SetSetting('_metasize',0); # Setting this forces WriteUtMetaRequest to request piece 0
				}
			}
			else {
				$self->WriteUtMetaRequest; # Request more ASAP
			}
		}
		elsif($this_offset > $this_psize){
			$self->warn($self->XID." sent unneeded metadata (MetaSize=>$metasize, ThisPsize=>$this_psize, Offset=>$this_offset, Len=>$this_payloadlen)");
		}
		# else -> 'old' (old) reply .. ignore it
		
	}
	
	##########################################################################
	# Set status
	sub SetStatus {
		my($self,$status) = @_;
		my $sx = $self->{main}->{Sockets}->{$self->{socket}} or $self->panic("Stale socket: $self->{socket}");
		$self->{status} = $status;
	}
	
	##########################################################################
	# Get status
	sub GetStatus {
		my($self,$status) = @_;
		my $sx = $self->{main}->{Sockets}->{$self->{socket}} or $self->panic("Stale socket: $self->{socket}");
		return $self->{status};
	}
	

	##########################################################################
	#
	sub SetExtensions {
		my($self,%args) = @_;
		foreach my $k (keys(%args)) {
			my $val = int($args{$k}||0);
			if($val == 0) {
				delete($self->{extensions}->{$k});
			}
			else {
				$self->{extensions}->{$k} = $val;
			}
		}
	}
	
	##########################################################################
	# Returns supported extensions
	sub GetExtension {
		my($self,$key) = @_;
		return $self->{extensions}->{$key};
	}
	
	
	
	
	
	##########################################################################
	# Parse Eproto Messages received from peers
	sub ParseEprotoMSG {
		my($self,$string) = @_;
		
		my $etype     = unpack("c",substr($string,0,1));
		my $bencoded  = substr($string,1);
		my $decoded   = $self->{super}->Tools->BencDecode($bencoded);
		if($etype == EP_HANDSHAKE) {
			foreach my $ext_name (keys(%{$decoded->{m}})) {
				if($ext_name eq "ut_pex") {
					$self->SetExtensions(UtorrentPex=>$decoded->{m}->{$ext_name});
				}
				elsif($ext_name eq "ut_metadata") {
					$self->debug($self->XID." Supports Metadata via $decoded->{m}->{$ext_name}");
					$self->SetExtensions(UtorrentMetadata=>$decoded->{m}->{$ext_name}, UtorrentMetadataSize=>$decoded->{metadata_size});
				}
				else {
					$self->debug($self->XID." Unknown eproto extension '$ext_name' (id: $decoded->{m}->{$ext_name})");
				}
			}
			
			if(defined($decoded->{e}) && $decoded->{e} != 0) {
				$self->SetExtensions(Encryption=>1);
			}
			
			if(defined($decoded->{reqq}) && $decoded->{reqq} > 0) {
				$self->SetRequestSlots($decoded->{reqq});
			}
			
			$self->SetRemotePort($decoded->{p});
		}
		elsif($etype == EP_UT_METADATA) {
			my $meta_type = $decoded->{msg_type};
			my $piece     = ($decoded->{piece} || 0);
			
			if($self->GetStatus == STATE_IDLE && $meta_type == UTMETA_REQUEST) {
				$self->AddUtMetaRequest($piece); # Queue request for metadata
			}
			elsif($self->GetStatus == STATE_NOMETA && $meta_type == UTMETA_DATA && $self->HasUtMetaRequest && $self->GetUtMetaRequest == $piece && length($bencoded)) {
				$self->StoreUtMetaData($bencoded);
			}
			elsif($meta_type == UTMETA_REJECT) {
				$self->warn($self->XID." rejected our metadata request");
			}
			else {
				$self->warn($self->XID." rejecting request (Type=>$meta_type, Piece=>$piece, State=>".$self->GetStatus.")");
				$self->WriteUtMetaReject($piece);
			}
		}
		elsif($etype == EP_UT_PEX && defined($decoded->{added})) {
			my @v4nodes = ();
			my @v6nodes = ();
			
			if(exists($decoded->{added})) {
				@v4nodes = $self->{super}->Tools->DecodeCompactIp($decoded->{added});
			}
			if(exists($decoded->{added6})) {
				@v6nodes = $self->{super}->Tools->DecodeCompactIpV6($decoded->{added6});
			}
			
			my @all_nodes = List::Util::shuffle(@v6nodes,@v4nodes);
			
			splice(@all_nodes, PEX_MAXACCEPT) if int(@all_nodes) >= PEX_MAXACCEPT;
			
			$self->{_super}->Torrent->GetTorrent($self->GetSha1)->AddNewPeers(@all_nodes);
		}
		else {
			$self->debug($self->XID." Ignoring message for unregistered/unsupported id: $etype");
		}
		
	}
	
	##########################################################################
	# 'Link' a SHA1 to this client
	sub SetSha1 {
		my($self,$sha1) = @_;
		my $sx = $self->{main}->{Sockets}->{$self->{socket}} or $self->panic("Stale socket: $self->{socket}");
		$self->panic("this client had it's own sha1 set: $self->{sha1}")  if  $self->{sha1};
		$self->panic("this client ($self->{socket} has no remote_ip set") if !$self->GetRemoteIp;
		$self->{sha1} = $sha1;
		$self->{_super}->Torrent->LinkTorrentToSocket($sha1,$self->GetOwnSocket);
		$self->{super}->Queue->IncrementStats($sha1, {'clients' => 1});
		$self->{main}->{IPlist}->{$self->GetRemoteIp}->{$sha1}++;
	}
	
	##########################################################################
	# Delink SHA1 from this client
	sub UnsetSha1 {
		my($self) = @_;
		my $sha1 = $self->GetSha1 or return undef;  # Sha1 was not registered
		
		$self->{_super}->Torrent->UnlinkTorrentToSocket($sha1, $self->GetOwnSocket);
		
		my $refcount = --$self->{main}->{IPlist}->{$self->GetRemoteIp}->{$sha1};
		
		if($refcount == 0) {
			delete($self->{main}->{IPlist}->{$self->GetRemoteIp}->{$sha1}); # Free memory
			if(int(keys(%{$self->{main}->{IPlist}->{$self->GetRemoteIp}})) == 0) {
				delete($self->{main}->{IPlist}->{$self->GetRemoteIp}); # ..we can also free memory here: no more connections to track for this ip
				$self->debug($self->GetRemoteIp." lost all connections");
			}
		}
		if($refcount < 0) {
			$self->panic("Refcount mismatch for ".$self->XID." : $refcount");
		}
	}
	
	##########################################################################
	# Get the SHA1 of this client
	sub GetSha1 {
		my($self) = @_;
		my $sx = $self->{main}->{Sockets}->{$self->{socket}} or $self->panic("Stale socket: $self->{socket}");
		return $self->{sha1};
	}

	
	##########################################################################
	# Set clients peerid (informal use only)
	sub SetRemotePeerID {
		my($self,$peerid) = @_;
		$self->{remote_peerid} = $peerid;
	}
	
	##########################################################################
	# Return clients peerid
	sub GetRemotePeerID {
		my($self) = @_;
		return $self->{remote_peerid};
	}
	
	##########################################################################
	# Return clients IP
	sub GetRemoteIp {
		my($self) = @_;
		return $self->{remote_ip} or $self->panic("No ip?!");
	}
	
	sub GetRemoteImplementation {
		my($self) = @_;
		my $ref = Bitflu::DownloadBitTorrent::ClientDb::decode($self->GetRemotePeerID);
		return $ref->{name}." ".$ref->{version};
	}
	
	##########################################################################
	# Return average-upload speed
	sub GetAvgSent {
		my($self) = @_;
		my $alive = ($self->{super}->Network->GetTime - $self->{kudos}->{born});
		return int( $self->{kudos}->{bytes_sent} / (1+$alive) );
	}
	
	##########################################################################
	# Return average-download speed
	sub GetAvgStored {
		my($self) = @_;
		my $alive = ($self->{super}->Network->GetTime - $self->{kudos}->{born});
		return int( $self->{kudos}->{bytes_stored} / (1+$alive) );
	}
	
	##########################################################################
	# Returns AverageStored in Percent (Can get above 100!)
	sub GetAvgStoredInPercent {
		my($self) = @_;
		my $hash    = $self->GetSha1 or return 0; # No torrent, no upload
		my $avg     = $self->GetAvgStored;
		my $global  = $self->{super}->Queue->GetStats($hash)->{speed_download};
		my $percent = ( ($avg/abs( int($global) || 1)) * 100 );
		return $percent;
	}
	
	##########################################################################
	# Returns AverageSent in Percent (Can get above 100!)
	sub GetAvgSentInPercent {
		my($self) = @_;
		my $hash    = $self->GetSha1 or return 0; # No torrent, no upload
		my $avg     = $self->GetAvgSent;
		my $global  = $self->{super}->Queue->GetStats($hash)->{speed_upload};
		my $percent = ( ($avg/abs( int($global) || 1)) * 100 );
		return $percent;
	}
	
	##########################################################################
	# Set bit as TRUE
	sub SetBit {
		my($self,$bitnum) = @_;
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		vec($self->{bitfield}->[$bfIndex],(7-$bitnum),1) = 1;
	}
	
	##########################################################################
	# Returns TRUE if bit is set, FALSE otherwise
	sub GetBit {
		my($self,$bitnum) = @_;
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		return vec($self->{bitfield}->[$bfIndex], (7-$bitnum), 1);
	}
	
	##########################################################################
	# Set bitfield of this client
	sub SetBitfield {
		my($self,$string) = @_;
		for(my $i=0; $i<length($string);$i++) {
			$self->{bitfield}->[$i] = substr($string,$i,1);
		}
	}
	
	##########################################################################
	# Returns a bitfield dump
	sub GetBitfield {
		my($self) = @_;
		return join('', @{$self->{bitfield}});
	}
	
	##########################################################################
	# Return the number of completed pieces of this peer
	sub GetNumberOfPieces {
		my($self) = @_;
		my $numpieces_peer = unpack("B*",$self->GetBitfield);
		   $numpieces_peer = ($numpieces_peer =~ tr/1//);
		return $numpieces_peer;
	}
	
	##########################################################################
	# Buffer for unfinished data
	sub AppendReadBuffer {
		my($self,$buffref, $bufflen) = @_;
		$self->{readbuff}->{buff}  .= ${$buffref};
		$self->{readbuff}->{len}   += $bufflen;
		return 1;
	}
	
	##########################################################################
	# Clean read buffer
	sub DropReadBuffer {
		my($self, $bytes) = @_;
		if($bytes < 0 or $self->{readbuff}->{len} == $bytes) {
			# Drop everything
			$self->{readbuff}->{buff} = '';
			$self->{readbuff}->{len}  = 0;
		}
		else {
			$self->{readbuff}->{buff} = substr($self->{readbuff}->{buff},$bytes);
			$self->{readbuff}->{len} -=$bytes;
		}
		$self->panic("Dropped too much data from ReadBuffer :-/") if $self->{readbuff}->{len} < 0;
		return $self->{readbuff}->{len};
	}
	
	##########################################################################
	# Returns the current read buffer
	sub GetReadBuffer {
		my($self) = @_;
		return(\$self->{readbuff}->{buff},$self->{readbuff}->{len});
	}
	
	##########################################################################
	# Unregister this client
	sub Destroy {
		my($self) = @_;
		$self->UnsetSha1;
		delete($self->{main}->{Sockets}->{$self->{socket}}) or $self->panic("Unable to destroy non-existant socket $self->{socket}");
	}
	
	
	
	
	# Protocol handler
	
	##########################################################################
	# Write handshake message to this client
	sub WriteHandshake {
		my($self) = @_;
		$self->{sha1} or $self->panic("No sha1 linked!");
		my $buff = pack("c",19);
		   $buff.= "BitTorrent protocol";
		   $buff.= $self->_assemble_extensions;
		   $buff.= pack("H40", $self->{sha1});
		   $buff.= $self->{_super}->{CurrentPeerId};
		$self->debug("$self : Wrote Handshake") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket},$buff);
	}
	
	##########################################################################
	# Send Eproto-Handshake to connected peer
	sub WriteEprotoHandshake {
		my($self, %args) = @_;
		
		my $eproto_data   = { reqq => MAX_OUTSTANDING_REQUESTS, e=>0, v=>$args{Version}, p=>$args{Port}, metadata_size => $args{Metasize},
		                      m => { ut_pex => int($args{UtorrentPex}), ut_metadata => int($args{UtorrentMetadata}) } };
		delete($eproto_data->{metadata_size}) if !$eproto_data->{metadata_size};
		my $xh = $self->{super}->Tools->BencEncode($eproto_data);
		my $buff =  pack("N", 2+length($xh));
		   $buff .= pack("c", MSG_EPROTO).pack("c", 0).$xh;
		$self->debug("$self : Wrote EprotoHandshake") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket},$buff);
	}
	
	##########################################################################
	# Send metadata to connected peer
	sub WriteUtMetaResponse {
		my($self, $piece) = @_;
		$self->panic("Piece is undef") unless defined($piece);
		
		my $this_offset     = $piece*UTMETA_CHUNKSIZE;
		my $this_torrent    = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		my $this_metasize   = $this_torrent->GetMetaSize;
		my $this_chunk_left = ($this_metasize-$this_offset);
		my $this_extindex   = $self->GetExtension('UtorrentMetadata');
		
		$self->debug($self->XID." Writing MetadataResponse (Piece=>$piece)") if PEER_DEBUG;
		
		if($this_chunk_left > 0 && $this_extindex > 0) {
			my $this_size     = ($this_chunk_left < UTMETA_CHUNKSIZE ? $this_chunk_left : UTMETA_CHUNKSIZE);
			my $this_bencoded = { msg_type=>UTMETA_DATA, piece=>$piece, total_size=>$this_metasize };
			delete($this_bencoded->{total_size}) if $piece != 0;
			my $payload_benc  = $self->{super}->Tools->BencEncode($this_bencoded);
			my $payload_data  = substr($this_torrent->GetMetaData, $this_offset, $this_size);
			$self->WriteEprotoMessage(Index=>$this_extindex, Payload=>$payload_benc.$payload_data);
		}
		else {
			$self->warn($self->XID." Cannot reply to request for piece $piece. !($this_chunk_left > 0 && $this_extindex > 0)");
		}
	}
	
	##########################################################################
	# Send utorrent metadata request
	sub WriteUtMetaRequest {
		my($self) = @_;
		
		$self->panic("WriteUtMetaRequest needs to get called at STATE_NOMETA!") if $self->GetStatus != STATE_NOMETA;
		
		my $torrent = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		if($torrent->GetMetaSize) { $self->panic("NOMETA client has MetaSize != 0"); }
		if($torrent->GetMetaSwap) { return;                                          } # Waits to get exchanged, do nothing
		
		if(my $peer_extid = $self->GetExtension('UtorrentMetadata')) {
			my $psize   = $torrent->Storage->GetSizeOfFreePiece(0);    # our current progress
			my $msize   = $torrent->Storage->GetSetting('_metasize');  # total size -> 0 if we do not know the size
			
			if($psize >= $msize) { # Something went 'wrong' -> Restart download
				$self->debug("metadata: requesting metadata of ".$self->GetSha1);
				$torrent->Storage->SetAsInwork(0); $torrent->Storage->Truncate(0); $torrent->Storage->SetAsFree(0);
				$torrent->Storage->SetSetting('_metasize',0);
				$psize = $msize = 0;
			}
			
			my $rqpiece = ($msize ? int($psize/UTMETA_CHUNKSIZE) : 0); # piece to request
			my $opcode  = $self->{super}->Tools->BencEncode({piece=>$rqpiece, msg_type=>UTMETA_REQUEST});
			
			$self->WriteEprotoMessage(Index=>$peer_extid, Payload=>$opcode);
			$self->AddUtMetaRequest($rqpiece);
			$self->debug("metadata: requesting piece $rqpiece from ".$self->XID);
		}
		else {
			$self->panic("You shall not call WriteUtMetaRequest for non ut_metadata peers");
		}
	}
	
	##########################################################################
	# Reject a request
	sub WriteUtMetaReject {
		my($self, $piece) = @_;
		if(my $peer_extid = $self->GetExtension('UtorrentMetadata')) {
			my $opcode = $self->{super}->Tools->BencEncode({piece=>$piece, msg_type=>UTMETA_REJECT});
			$self->WriteEprotoMessage(Index=>$peer_extid, Payload=>$opcode);
			$self->debug($self->XID." sent rejection for $piece via $peer_extid");
		}
		else {
			$self->warn($self->XID." asked for metadata but forgot to activate extension");
		}
	}
	
	##########################################################################
	# Write our current bitfield to this client
	sub WriteBitfield {
		my($self) = @_;
		
		my $tobj = $self->{_super}->Torrent->GetTorrent($self->{sha1}) or $self->panic("No torrent!");
		my $bitfield = $tobj->GetFakeBitfield;
		my $buff = pack("N", 1+length($bitfield));
		   $buff.= pack("c", MSG_BITFIELD);
		   $buff.= $bitfield;
		$self->debug("$self : Wrote Bitfield");
		return $self->{super}->Network->WriteData($self->{socket},$buff);
	}
	
	##########################################################################
	# Send a NOOP message
	sub WritePing {
		my($self) = @_;
		$self->debug("$self : Wrote PING") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket}, pack("cccc",0000));
	}
	

	sub WriteInterested {
		my($self) = @_;
		$self->SetInterestedME;
		$self->debug("$self : Wrote INTERESTED") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_INTERESTED));
	}

	sub WriteUninterested {
		my($self) = @_;
		$self->SetUninterestedME;
		$self->debug("$self : Wrote -UN-INTERESTED") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_UNINTERESTED));
	}
	
	sub WriteUnchoke {
		my($self) = @_;
		$self->panic("Cannot UNchoke an unchoked peer") if !$self->GetChokePEER;
		$self->SetUnchokePEER;
		$self->debug("$self : Unchoked peer") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_UNCHOKE));
	}
	
	sub WriteChoke {
		my($self) = @_;
		$self->panic("Cannot choke a choked peer") if $self->GetChokePEER;
		$self->SetChokePEER;
		$self->debug("$self : Choked peer") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_CHOKE));
	}
	
	sub WriteHave {
		my($self,$piece) = @_;
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",5).pack("c", MSG_HAVE).pack("N",$piece));
	}

	sub WritePiece {
		my($self, %args) = @_;
		my $x .= pack("N", 9+$args{Size});
		$x    .= pack("c", MSG_PIECE);
		$x    .= pack("N", $args{Index});
		$x    .= pack("N", $args{Offset});
		$x    .= ${$args{Dataref}};
		$self->debug("$self : Delivering to client: Index=>$args{Index}") if PEER_DEBUG;
		$self->{kudos}->{bytes_sent} += $args{Size};
		return $self->{super}->Network->WriteData($self->{socket}, $x);
	}
	
	#################################################
	# Send MSG_PORT message to peer
	sub WriteDhtPort {
		my($self,$port) = @_;
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",3).pack("c", MSG_PORT).pack("n",$port));
	}
	
	#################################################
	# Sendout Eproto message
	sub WriteEprotoMessage {
		my($self, %args) = @_;
		$args{Index} or $self->panic("No index!");
		my $x .= pack("N", 2+length($args{Payload}));
		   $x .= pack("c", MSG_EPROTO);
		   $x .= pack("C", int($args{Index}));
		   $x .= $args{Payload};
		return $self->{super}->Network->WriteData($self->{socket},$x);
	}
	
	
	
	sub WriteRequest {
		my($self, %args) = @_;
		my $x = pack("N",13);
		$x .= pack("c", MSG_REQUEST);
		$x .= pack("N", $args{Index});
		$x .= pack("N", $args{Offset});
		$x .= pack("N", $args{Size});
		
		$self->panic("EMPTY SIZE?!") if $args{Size} <= 0;
		
		$self->LockPiece(Index=>$args{Index}, Offset=>$args{Offset}, Size=>$args{Size});
		$self->SetLastRequestTime;
		$self->debug($self->XID." : Request { Index => $args{Index} , Offset => $args{Offset} , Size => $args{Size} }") if PEER_DEBUG;
		return $self->{super}->Network->WriteData($self->{socket}, $x);
	}
	
	sub _assemble_extensions {
		my($h) = @_;
		my $ext = "0" x 64;
		
		if(1) {
			#Enables Enhanced Messages
			substr($ext,43,1,1);
		}
		if(1) {
			# advertise DHT
			substr($ext,63,1,1);
		}
		return pack("B64",$ext);
	}
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug("BT-peer : ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info("BT-peer : ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn("BT-peer : ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic("BT-peer : ".$msg); }
	
1;

package Bitflu::DownloadBitTorrent::ClientDb;

	my $cdef = { '?'  => { name => 'Unknown:',  vm => [0..7]                       }, ''   => { name => '......'                                       },
	             'BC' => { name => 'BitComet',  vm => [0], vr => [1], vp => [2..3] }, 'BCL'=> { name => 'BitLord',  vm => [0], vr => [1], vp => [2..3] },
	             'BF' => { name => 'Bitflu',    vm => [0..3]                       }, 'DE' => { name => 'Deluge',   vm => [0], vr => [1], vp => [2..3] },
	             'AZ' => { name => 'Azureus',   vm => [0], vr => [1], vp => [2..3] }, 'UT' => { name => 'uTorrent', vm => [0], vr => [1], vp => [2..3] },
	             'KT' => { name => 'KTorrent',  vm => [0], vr => [1], vp => [2..3] }, 'TR' => { name => 'Transmission', vm => [0], vr => [1], vp => [2..3] },
	             'BS' => { name => 'BitSpirit',                                    }, 'XL' => { name => 'Xunlei',   vm => [0], vr => [1], vp => [2..3] },
	             'M'  => { name => 'Mainline',  vm => [0], vr => [2], vp => [4..4] }, 'FG' => { name => 'FlashGet', vm => [0], vr => [1..2]            },
	             'T'  => { name => 'BitTornado',vm => [0..4]                       }, 'S'  => { name => 'Shad0w',   vm => [0..4]                       },
	             'SD' => { name => 'Thunder',  vm => [0], vr => [1], vp => [2..3]  }, 'UM' => { name => 'uTorrent Mac', vm => [0], vr => [1], vp => [2..3] },
	             'LT' => { name => 'libtorrent',                                   }, 'lt' => { name => 'libTorrent',                                  },
	             'SP' => { name => 'BitSpirit', vm => [0], vr => [1..2]            }, 'XX' => { name => 'Xtorrent', vm => [0], vr => [1], vp=>[2..3]   },
	             'QD' => { name => 'QQDownload', vm => [0], vr => [1], vp=>[2..3]  }, 'ML' => { name => 'mlDonkey', vm => [0], vr => [2], vp=>[3]      },
	             'LP' => { name => 'Lphant', vm => [0], vr => [1], vp=>[2..3]      }, 'AG' => { name => 'Ares',     vm => [0], vr => [1], vp=>[2..3]   },
	             'BE' => { name => 'BT-DevSDK',                                    },
	           };

	sub decode {
		my($string) = @_;
		
		my $client_brand   = '';
		my $client_version = '';
		
		if(!$string) {
			$client_brand   = '';
			$client_version = '';
		}
		elsif( $string =~ /UDP0$/) { $client_brand = 'BS' }                                     # Funky BitSpirit
		elsif( $string =~ /^exbc(.)(.)(....)/) {
			$client_version = unpack("H",$1).unpack("H",$2)."00";
			$client_brand   = 'BC';
			$client_brand   = 'BCL' if $3 eq 'LORD';
		}
		elsif(($client_brand, $client_version) = $string =~ /^-(\w\w)(\S{4})-/) {}                # Azureus-Style
		elsif(($client_brand, $client_version) = $string =~ /^(M)(\d-\d-\d-)/)  {}                # Mainline
		elsif(($client_brand, $client_version) = $string =~ /^-(\w\w)(\S{3})/)  {}                # FlashGet-Style
		elsif(($client_brand, $client_version) = $string =~ /^([A-Z])([A-Za-z0-9+=\.-]{5})/) { }  # Shad0w
		else {
			$client_brand   = "?";
			$client_version = $string;
		}
		
		my $ref  = ( $cdef->{$client_brand} || $cdef->{'?'} );
		my $name = $ref->{name};
		my $vers = '';
		
		foreach my $a ($ref->{vm}, $ref->{vr}, $ref->{vp} ) {
			next unless $a;
			foreach my $xoff (@$a) { $vers .= (length($client_version) > $xoff ? substr($client_version,$xoff,1) : '') }
			$vers .= ".";
		}
		chop $vers;
		
		$vers =~ tr/0-9A-Za-z\.//cd; # No funky stuff here please
		$name .= " - $client_brand" if $name =~ /Unknown/;
		
		return{name =>$name, version => $vers};
	}

1;


