package Bitflu::DownloadBitTorrent;
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

#
# Mit dem neuen TIMEOUT_PIECE_FAST könnten wir ein endgame simulieren:
# Wenn wir nur noch so ~20 pieces brauchen (fixme: der timeouter sollte das nicht selber berechnen. es sollte ein $torrent->IsEndgame geben)
# könnten wir auch daten von nicht (mehr?) akzeptierten peers fressen
# Die aktive session müssten wir dazu nichtmal umbedingt killen, da wir dies sowieso später machen (wenn es timeouted ist)
#
#
# Fixme: Seeder mode. Der run sollte SetSetting 'completed' auf 1 setzen und zu seeden beginnen
#
# Fixme: Irgendwie machen wir das interested-senden nicht so klug. Wir sollten nicht sofort / immer ein writeUninterested senden, oder?
#
#
# Fixme: Die Piece-Migration ist soweit nett, aber wir sollten nicht alle pieces akzeptieren, nur weil wir fast alles haben:
# Viel besser wär' ein 'bad-times' und ein 'good-times' mode.
#
# Im bad-times-mode würden wir, wenn wir ein piece migrieren, es in einem hash als allow_from_everyone oder so marken
# und pieces von überall akzeptieren
# Beim switch in den goot-times-mode clearen wir den dann und markieren migrationen auch nicht
#
# So könnten wir ein 'dynamic-endgame' machen
#

#
# Fixme: Wir sollten beim Start (?) 'HotPieces' berechnen. Somit kriegen wir möglichst schnell einige dingsens
#
#

use strict;
use Digest::SHA1;
use List::Util;

use constant SHALEN   => 20;
use constant BTMSGLEN => 4;

use constant BUILDID => '7C10';  # YMDD (Y+M => HEX)

use constant STATE_READ_HANDSHAKE    => 200;  # Wait for clients Handshake
use constant STATE_READ_HANDSHAKERES => 201;  # Read clients handshake response
use constant STATE_IDLE              => 300;  # Connection with client fully established


use constant MSG_CHOKE          => 0;
use constant MSG_UNCHOKE        => 1; # Implemented
use constant MSG_INTERESTED     => 2; # Implemented
use constant MSG_UNINTERESTED   => 3; # Implemented
use constant MSG_HAVE           => 4; # Implemented
use constant MSG_BITFIELD       => 5; # Implemented
use constant MSG_REQUEST        => 6; # Implemented
use constant MSG_PIECE          => 7; # Implemented
use constant MSG_CANCEL         => 8; # Implemented
use constant MSG_WANT_METAINFO  => 10; # Unused
use constant MSG_METAINFO       => 11; # Unused
use constant MSG_SUSPECT_PIECE  => 12; # Unused
use constant MSG_SUGGEST_PIECE  => 13; # FastPeers (Not implemented)
use constant MSG_HAVE_ALL       => 14; # FastPeers   | IMPLEMENTED  (Recv)       (Silly extension: no need to send this)
use constant MSG_HAVE_NONE      => 15; # FastPeers   | IMPLEMENTED  (Recv)       (Silly extension: no need to send this)
use constant MSG_REJECT_REQUEST => 16; # FastPeers (Not implemented)
use constant MSG_ALLOWED_FAST   => 17; # FastPeers (Not implemented) (parsed but unused)
use constant MSG_HOLE_PUNCH     => 18; # NAT-Unused
use constant MSG_UTORRENT_MSG   => 19; # Unused (??)
use constant MSG_EPROTO         => 20;

use constant TIMEOUT_NOOP          => 110;    # Ping each 110 seconds
use constant TIMEOUT_FAST          => 20;     # Fast timeouter (wait for bitfield, handshake, etc)
use constant TIMEOUT_UNUSED_CLIENT => 1200;   # Drop connection if we didn't send/recv a piece within 20 minutes ('deadlock' connection)
use constant TIMEOUT_PIECE_NORM    => 90;    # How long we are going to wait for a piece in 'normal' mode
use constant TIMEOUT_PIECE_FAST    => 20;     # How long we are going to wait for a piece in 'almost done' mode

use constant DELAY_FULLRUN         => 13;     # How often we shall save our configuration and rebuild the have-map
use constant DELAY_PPLRUN          => 600;    # How often shall we re-create the PreferredPiecesList ?
use constant DELAY_CHOKEROUND      => 30;     # How often shall we run the unchoke round?
use constant TIMEOUT_HUNT          => 182;    #
use constant EP_UT_PEX => 1;

use constant PEX_MAXPAYLOAD => 32; # Limit how many clients we are going to send

##########################################################################
# Register BitTorrent support
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, phunt => { phi => 0, phclients => [], lastchokerun => 0, lastpplrun => 0, lastrun => 0,
	                                             fullrun => 0, chokemap => { can_choke => {}, can_unchoke => {} }, havemap => {}, pexmap => {} } };
	bless($self,$class);
	
	$self->{Dispatch}->{Torrent} = Bitflu::DownloadBitTorrent::Torrent->new(super=>$mainclass, _super=>$self);
	$self->{Dispatch}->{Peer}    = Bitflu::DownloadBitTorrent::Peer->new(super=>$mainclass, _super=>$self);
	$self->{CurrentPeerId}       = pack("H*",unpack("H40", "-BF".BUILDID."-".sprintf("#%X%X",int(rand(0xFFFFFFFF)),int(rand(0xFFFFFFFF)))));
	
	my $cproto = { torrent_port => 6688, torrent_bind => 0, torrent_minpeers => 15, torrent_maxpeers => 60,
	               torrent_upslots => 10, torrent_importdir => $mainclass->Configuration->GetValue('workdir').'/import',
	               torrent_gcpriority => 5,
	               torrent_totalpeers => 400, torrent_maxreq => 6 };
	
	foreach my $funk qw(torrent_maxpeers torrent_minpeers torrent_gcpriority torrent_upslots torrent_maxreq) {
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
	                                                    MaxPeers=>$mainclass->Configuration->GetValue('torrent_totalpeers'), Throttle=>1);
	
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
	$self->{super}->Admin->RegisterCommand('bt_connect', $self, 'CreateNewOutgoingConnection', "Creates a new bittorrent connection",
	[ [undef, "Usage: bt_connect queue_id ip port"],
	  [undef, ""],
	  [undef, "This command can be used to forcefully establish a connection with a known peer"]
	]);
	$self->{super}->Admin->RegisterCommand('load', $self, 'LoadTorrentFromDisk'              , "Start downloading a new .torrent file",
	[ [undef, "1: Store the torrent file in a directory readable by bitflu (watchout for chroot and permissions)"],
	  [undef, "2: Type: 'load /patch/to/torrent.torrent'                   (avoid whitespaces)"],
	  [undef, "Hint: You can also place torrent into the 'autoload' folder. Bitflu will pickup the files itself"],
	] );
	
	$self->{super}->Admin->RegisterCommand('iplist', $self, 'XXX_IPLIST', 'Show ip<->sha1 breakdown');
	$self->{super}->Admin->RegisterCommand('dbg_vrfy', $self, '_Command_Verify', '(ADVANCED) Verify pieces of given sha1 , blocks bitflu until finished!');
	$self->{super}->Admin->RegisterCommand('import_torrent', $self, '_Command_ImportTorrent', '(ADVANCED) Import torrent from torrent_importdir');
	
	
	unless(-d $self->{super}->Configuration->GetValue('torrent_importdir')) {
		$self->debug("Creating torrent_importdir '".$self->{super}->Configuration->GetValue('torrent_importdir')."'");
		mkdir($self->{super}->Configuration->GetValue('torrent_importdir')) or $self->panic("Unable to create torrent_importdir : $!");
	}
	
	
	$self->info("BitTorrent plugin loaded. Using tcp port ".$self->{super}->Configuration->GetValue('torrent_port'));
	return 1;
}

sub _Command_ImportTorrent {
	my($self, $sha1) = @_;
	
	my @A       = ();
	my $so      = $self->{super}->Storage->OpenStorage($sha1);
	my $pfx     = $self->{super}->Configuration->GetValue('torrent_importdir');
	
	
	if($so) {
		my $torrent = $self->Torrent->GetTorrent($sha1) or $self->panic("Unable to open torrent for $sha1");
		my $cs = $so->GetSetting('size') or $self->panic("$sha1 has no size setting");
		my $fl = ();
		
		my $fake_peer = $self->Peer->AddNewClient($self, {Port=>0, Ipv4=>'-internal-'});
		$fake_peer->SetSha1($sha1);
		$fake_peer->SetBitfield(pack("B*", ("1" x length(unpack("B*",$torrent->GetBitfield)))));
		
		foreach my $this_fo (split(/\n/,$so->GetSetting('filelayout'))) {
			my($this_path, $this_start,$this_end) = split(/\0/,$this_fo);
			my @a_clean    = ();
			my $path_raw   = $pfx;
			my $path_clean = $pfx;
			# Get a clean path
			foreach(split('/',$this_path)) { next if $_ eq ".."; next if length == 0; push(@a_clean,$_); }
			
			$path_raw   .= '/'.$this_path;
			$path_clean .= '/'.join('/',@a_clean);
			
			my $i_raw   = ( (stat($path_raw))[1]   || 0);
			my $i_clean = ( (stat($path_clean))[1] || 0);
			
			if($i_raw == 0 && $i_clean == 0) {
				$self->warn("import: '$path_clean' does not exist, skipping file");
			}
			elsif($i_raw != $i_clean) {
				$self->warn("import: Obscure path '$path_raw' doesn't point to the same file as '$path_clean', skipping file");
			}
			else {
				$fl->{$this_start} = { path=>$path_clean, start=>$this_start, end=>$this_end };
			}
		}
		
		# Need to sort this, because we can only do streams
		foreach my $ckey (sort({ $a <=> $b} keys(%$fl))) {
			my $r = $fl->{$ckey};
			
			for(my $i = $r->{start}; $i < $r->{end};) {
				my $piece_to_use = int($i/$cs);
				my $piece_offset = $i - $piece_to_use*$cs;
				my $canread      = ($r->{end}-$i     < $cs       ? ($r->{end}-$i)      : $cs);
				   $canread      = ($cs-$piece_offset < $canread ? ($cs-$piece_offset) : $canread);
				$i+=$canread;
				
				if ($so->IsSetAsFree($piece_to_use) && !$torrent->TorrentwidePieceLockcount($piece_to_use) && open(FEED, "<", $r->{path}) ) {
					$self->warn("Importing from local disk: Piece=>$piece_to_use, Size=>$canread, Offset=>$piece_offset, Path=>$r->{path}");
					my $buff = '';
					seek(FEED, $i-$canread-$r->{start},0) or $self->panic("SEEK FAIL : $!");
					my $didread = sysread(FEED,$buff,$canread);
					close(FEED);
					$fake_peer->LockPiece(Index=>$piece_to_use, Offset=>$piece_offset, Size=>$didread);
					$so->Truncate($piece_to_use) if $piece_offset == 0;
					$fake_peer->StoreData(Index=>$piece_to_use, Offset=>$piece_offset, Size=>$didread, Dataref=>\$buff, DisableHunt=>1);
				}
				
			}
			
		}
		$self->_Network_Close($self);
		push(@A, [1, "$sha1 : Import finished."]);
	}
	else {
		push(@A, [1, "'$sha1' does not exist"]);
	}
	
	return({CHAINSTOP=>1, MSG=>\@A});
}

sub _Command_Verify {
	my($self, $sha1) = @_;
	
	my @A  = ();
	my $cs = 1;
	
	my $so = $self->{super}->Storage->OpenStorage($sha1);
	my $errors  = 0;
	
	if($so) {
		my $torrent = $self->Torrent->GetTorrent($sha1) or $self->panic("Unable to open storage for $sha1");
		for my $cc (0..$so->GetSetting('chunks')) {
			next unless $so->IsSetAsDone($cc);
			$so->SetAsInworkFromDone($cc);
			
			if( $self->Peer->VerifyOk(Torrent=>$torrent, Index=>$cc, Size=>$so->GetSizeOfInworkPiece($cc)) ) {
				$so->SetAsDone($cc);
				$self->info("VERIFIED: $cc \@ $sha1 matches checksum");
			}
			else {
				$self->warn("BAD PIECE: $cc \@ $sha1 is corrupted. Truncating piece");
				$so->Truncate($cc);
				$so->SetAsFree($cc);
				$errors++;
			}
		}
		push(@A, [1, "Verification of $sha1 finishied"]);
	}
	else {
		push(@A, [undef, "$sha1 does not exist"]);
		$cs = 0;
	}
	
	if($errors) {
		$self->stop("Bitflu found some corrupted pieces while checking $sha1. You must restart bitlfu now");
	}
	
	return({CHAINSTOP=>$cs, MSG=>\@A});
}


sub XXX_IPLIST {
	my($self) = @_;
	
	my @A = ();
	
	my $x = $self->Peer->{IPlist};
	
	foreach my $ip (keys(%$x)) {
		push(@A, [undef, "IPv4: $ip"]);
		foreach my $sha1 (keys(%{$x->{$ip}})) {
			push(@A, [1, "   --> $sha1 == $x->{$ip}->{$sha1}"]);
		}
		push(@A, [undef, '']);
	}
	
	
	return({CHAINSTOP=>1, MSG=>\@A});
}



##########################################################################
# Load / Resume a torrent file
sub resume_this {
	my($self, $sid) = @_;
	my $so             = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage $sid : $!");
	my $rdata          = $so->GetSetting('_torrent')                or $self->panic("Storage $sid has no torrent object!");
	my $href           = Bitflu::DownloadBitTorrent::Bencoding::decode($rdata);
	return undef if(ref($href) ne "HASH");
	
	my $torrent     = $self->Torrent->AddNewTorrent(StorageId=>$sid, Torrent=>$href);
	my $total_bytes = (($so->GetSetting('chunks')-1) * $so->GetSetting('size')) + ($so->GetSetting('size') - $so->GetSetting('overshoot'));
	my $done_bytes  = 0;
	my $done_chunks = 0;
	
	for my $cc (0..$so->GetSetting('chunks')) {
		# Fixme: Wir sollten hier auch checken, ob: wir 'lost' chunks haben
		# ..oder ob wir doppelte haben
		# ..oder ob wir ganze pieces in 'free' und 'done' haben die wir truncaten sollten und in .free pappen
		if($so->IsSetAsInwork($cc) && !($so->IsSetAsFree($cc))) {
			$so->SetAsFree($cc);
		}
		elsif($so->IsSetAsDone($cc)) {
			$torrent->SetBit($cc);
			$done_bytes += $so->GetSizeOfDonePiece($cc);
			$done_chunks++;
		}
	}
	
	$self->{super}->Queue->SetStats($sid, {total_bytes=>$total_bytes, done_bytes=>$done_bytes, uploaded_bytes=>int($so->GetSetting('_uploaded_bytes')),
	                                       active_clients=>0, clients=>0,
	                                       piece_migrations=>int($so->GetSetting('_piece_migrations')),
	                                       total_chunks=>int($so->GetSetting('chunks')), done_chunks=>$done_chunks});
	$torrent->SetStatsUp(0); $torrent->SetStatsDown(0);
	return 1;
}

##########################################################################
# Drop a torrent
sub cancel_this {
	my($self, $sid) = @_;
	
	# Ok, this is a bit tricky: First get the torrent itself
	my $this_torrent = $self->Torrent->GetTorrent($sid) or $self->panic("Torrent $sid does not exist!");
	
	# And close down the TCP connection with ALL clients linked to thisone:
	foreach my $con ($this_torrent->GetPeers) {
		$self->KillClient($self->Peer->GetClient($con));
	}
	
	# .. now remove the information about this SID from this module ..
	$self->Torrent->DestroyTorrent($sid);
	# .. and tell the queuemgr to drop it also
	$self->{super}->Queue->RemoveItem($sid);
}


##########################################################################
# Mainrunner


sub run {
	my($self) = @_;
	$self->{super}->Network->Run($self, {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	
	my $NOW = $self->{super}->Network->GetTime;
	my $PH  = $self->{phunt};
	
	return if $PH->{lastrun} == $NOW;
	$PH->{lastrun} = $NOW;
	$PH->{credits} = (abs(int($self->{super}->Configuration->GetValue('torrent_gcpriority'))) or 1);
	
	if($PH->{phi} == 0) {
		my @a_clients     = List::Util::shuffle($self->Peer->GetClients);
		$PH->{phclients}  = \@a_clients;
		$PH->{phi}        = int(@a_clients);
		$PH->{havemap}    = {};  # Clear HaveFlood map
		
		if($PH->{fullrun} <= $NOW-(DELAY_FULLRUN)) {
			my $drift = (int($NOW-$PH->{fullrun}) or 1);
			my $DOPPL = ($PH->{lastpplrun} <= $NOW-(DELAY_PPLRUN) ? 1 : 0);
			$PH->{pexmap}     = {};  # Clear PEX-Map
			$PH->{fullrun}    = $NOW;
			$PH->{lastpplrun} = $NOW if $DOPPL;
			$self->warn("*** ISSUING A FULLRUN ($drift) **");
			foreach my $torrent ($self->Torrent->GetTorrents) {
				my $tobj    = $self->Torrent->GetTorrent($torrent);
				my $so      = $self->{super}->Storage->OpenStorage($torrent) or $self->panic("Unable to open storage for $torrent");
				
				foreach my $persisten_stats qw(uploaded_bytes piece_migrations) {
					$so->SetSetting("_".$persisten_stats, $self->{super}->Queue->GetStats($torrent)->{$persisten_stats});
				}
				
				my @a_haves = $tobj->GetHaves; $tobj->ClearHaves;  # ReBuild HaveMap
				$PH->{havemap}->{$torrent} = \@a_haves;
				my $bps_up  = $tobj->GetStatsUp/$drift;
				my $bps_dwn = $tobj->GetStatsDown/$drift;
				$tobj->SetStatsUp(0); $tobj->SetStatsDown(0);
				$self->{super}->Queue->SetStats($torrent, {speed_upload => $bps_up, speed_download => $bps_dwn });
				
				if($DOPPL && !$tobj->IsComplete) {
					$self->warn("ReCreating PP-List of $torrent");
					$tobj->GenPPList;
				}
			}
		}
		
		if($PH->{lastchokerun} <= $NOW-(DELAY_CHOKEROUND) && ($PH->{lastchokerun} = $NOW)) {
			print Data::Dumper::Dumper($PH->{chokemap});
			my $CAM    = $PH->{chokemap}->{can_unchoke};
			my @sorted = sort { $CAM->{$b} cmp $CAM->{$a} } keys %$CAM; 
			print Data::Dumper::Dumper(\@sorted);
			
			my $CAN_UNCHOKE = (abs(int($self->{super}->Configuration->GetValue('torrent_upslots'))) or 1);
			my $DID_UNCHOKE = {};
			foreach my $this_name (@sorted) {
				last if --$CAN_UNCHOKE < 0;
				next unless $self->Peer->ExistsClient($this_name);
				print "=> UNCHOKING $this_name\n";
				$self->Peer->GetClient($this_name)->WriteUnchoke unless $PH->{chokemap}->{can_choke}->{$this_name};
				$DID_UNCHOKE->{$this_name} = 1;
			}
			
			print "=> CURRENTLY UNCHOKED CLIENTS:  ".Data::Dumper::Dumper($DID_UNCHOKE);
			
			foreach my $this_name (keys(%{$PH->{chokemap}->{can_choke}})) {
				next if $DID_UNCHOKE->{$this_name};
				next unless $self->Peer->ExistsClient($this_name);
				next if $self->Peer->GetClient($this_name)->GetChokePEER; # Client could have choked itself via MSG_UNINTERESTED
				print "=> CHOKING $this_name\n";
				$self->Peer->GetClient($this_name)->WriteChoke;
			}
			$PH->{chokemap} = { can_choke => {}, can_unchoke => {} };
		}
	}
	
	while($PH->{phi} > 0) {
		my $c_index = --$PH->{phi};
		my $c_sname = ${$PH->{phclients}}[$c_index];
		
		next unless   $self->Peer->ExistsClient($c_sname); # Client vanished
		last if --$PH->{credits} < 0;
		print "PHI: $c_sname\n";
		my $c_obj    = $self->Peer->GetClient($c_sname);
		my $c_lastio = $c_obj->GetLastIO;
		
		if($c_obj->GetStatus != STATE_IDLE) {
			# Client didn't complete handshake yet.. do a fast-timeout
			if($c_lastio < ($NOW - TIMEOUT_FAST)) {
				$self->warn("<$c_obj> did not complete handshake, dropping connection");
				$self->KillClient($c_obj);
			}
		}
		else {
			my $c_sha1    = $c_obj->GetSha1 or $self->panic("<$c_obj> has no sha1 bound");
			my $c_torrent = $self->Torrent->GetTorrent($c_sha1);
			
			if($c_obj->GetLastUsefulTime < ($NOW-(TIMEOUT_UNUSED_CLIENT))) {
				$self->warn("<$c_obj> : Dropping connection with silent client");
				$self->KillClient($c_obj);
				next;
			}
			
			if($c_lastio < ($NOW-(TIMEOUT_NOOP))) {
				$self->warn("<$c_obj> : Sending NOOP");
				$c_obj->WritePing;
			}
			
			foreach my $this_piece (@{$PH->{havemap}->{$c_sha1}}) {
				$self->debug("HaveFlooding $c_obj : $this_piece");
				$c_obj->WriteHave($this_piece);
			}
			
			if(!defined($PH->{pexmap}->{$c_sha1}) && $c_obj->GetExtension('UtorrentPex')) {
				$PH->{pexmap}->{$c_sha1} = 1;
				$self->warn("Sending PEX for $c_sha1");
				$self->_AssemblePexForClient($c_obj,$c_torrent) if $c_torrent->IsPrivate == 0;
			}
			
			if(!$c_torrent->IsComplete) {
				my $last_received = $c_obj->GetLastDownloadTime;
				my $this_timeout  = ($c_torrent->IsAlmostComplete ? TIMEOUT_PIECE_FAST : TIMEOUT_PIECE_NORM);
				my $this_hunt     = 1;
				if($last_received+$this_timeout <= $NOW) {
					foreach my $this_piece (keys(%{$c_obj->GetPieceLocks})) {
						$self->{super}->Queue->IncrementStats($c_sha1, {piece_migrations => 1});
						$c_obj->ReleasePiece(Index=>$this_piece);
						$c_obj->AdjustRanking(-2);
						$this_hunt = 0;
						$self->warn($c_obj->XID." -> Released piece $this_piece ($last_received+$this_timeout <= $NOW)");
					}
				}
				# Fixme: HuntPiece könnte sich merken, ob wir vor 20 sekunden oder so schon mal da waren und die request rejecten
				# ev. könnte SetBit und SetBitfield ein 'needs_hunt' field triggern
				
				
				if($this_hunt && ($c_obj->GetLastHunt < ($NOW-(TIMEOUT_HUNT))) ) {
					$self->warn("$c_obj : hunting");
					$c_obj->HuntPiece;
				}
			}
			
			if(!$c_obj->GetChokePEER) {
				# Peer is unchoked, we could choke it
				$PH->{chokemap}->{can_choke}->{$c_sname} = $c_sname;
			}
			
			if($c_obj->GetInterestedPEER) {
				# Peer is choked and interested, we could unchoke it
				my $ranking = $c_obj->GetRanking;
				$PH->{chokemap}->{can_unchoke}->{$c_sname} = $ranking;
			}
			
		}
	}
	
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
	
	my $xref  = {dropped=>'', added=>'', 'added.f'=>''};  # Hash to send
	my $pexc  = 0;                                        # PexCount
	my $pexid = $client->GetExtension('UtorrentPex');     # ID we are using to send message
	
	foreach my $cid ($torrent->GetPeers) {
		my $cobj                     = $self->Peer->GetClient($cid);
		next if $cobj->GetStatus     != STATE_IDLE;     # Still connecting to this peer
		next if $cobj->{remote_port} == 0;              # We don't know the remote port -> can't publish this contact
		last if ++$pexc              >= PEX_MAXPAYLOAD; # Maximum payload reached, stop search
		
		map($xref->{'added'} .= pack("C",$_), split(/\./,$cobj->{remote_ip},4));
		$xref->{'added'}     .= pack("n",$cobj->{remote_port});
		$xref->{'added.f'}   .= chr( ( defined($cobj->GetExtension('Encryption')) ? 1 : 0 ) ); # 1 if client told us that it talks silly-encrypt
	}
	
	if($pexc && $pexid) {
		# Found some clients -> send it to the 'lucky' peer :-)
		$self->debug($client->XID." Sending $pexc pex nodes");
		$client->WriteEprotoMessage(Index=>$pexid, Message=>$xref);
	}
	
}


##########################################################################
# Load a .torrent file from disk and init the storage
sub LoadTorrentFromDisk {
	my($self, @args) = @_;
	
	my $hits = 0;
	my @A = ();
	
	foreach my $file (@args) {
		my $ref = Bitflu::DownloadBitTorrent::Bencoding::torrent2hash($file);
		if(defined($ref->{torrent_hash})) {
				my $numpieces  = (length($ref->{content}->{info}->{pieces})/SHALEN);
				my $piecelen   = $ref->{content}->{info}->{'piece length'};
				my $filelayout = ();
				my $xtotalsize = $numpieces * $piecelen;
				my $overshoot  = undef;
				my $ccsize     = 0;
				
				if($numpieces < 1) {
					push(@A, [2, "file $file has no valid SHA1 string, skipping corrupted torrent"]);
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
					push(@A, [2, "file $file is missing size information, skipping corrupted torrent"]);
					next;
				}
				
				
				my $so = $self->{super}->Queue->AddItem(Name=>$ref->{content}->{info}->{name}, Chunks=>$numpieces, Overshoot=>$overshoot,
				                                          Size=>$piecelen, Owner=>$self, ShaName=>$ref->{torrent_hash}, FileLayout=>$filelayout);
				if(defined($so)) {
					$so->SetSetting('_torrent', $ref->{torrent_data})        or $self->panic("Unable to store torrent file as setting : $!");
					$so->SetSetting('type', ' bt ')                          or $self->panic("Unable to store type setting : $!");
					$self->resume_this($ref->{torrent_hash});
					push(@A, [1, "file $file loaded as BitTorrent with SHA1 $ref->{torrent_hash}"]);
				}
				else {
					push(@A, [2, "file $file failed to load: $ref->{torrent_hash} seems to exist in queue"]);
				}
				$hits++;
		}
	}
	return({CHAINSTOP=>$hits, MSG=>\@A});
}

##########################################################################
# Create a new connection to a peer
sub CreateNewOutgoingConnection {
	my($self,$hash,$ip,$port) = @_;
	
	my $msg = "torrent://$hash/nodes/$ip:$port";
	if($hash && $self->Torrent->GetTorrent($hash) && $port) {
		if($self->{super}->Configuration->GetValue('torrent_minpeers') > $self->{super}->Queue->GetStats($hash)->{clients}) {
			my $sock   = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$port, Ipv4=>$ip, Timeout=>5) or return undef;
			my $client = $self->Peer->AddNewClient($sock, {Port=>$port, Ipv4=>$ip});
			$client->SetSha1($hash);
			$client->WriteHandshake;
			$client->SetStatus(STATE_READ_HANDSHAKERES);
			$msg .= " established";
			
			if($client->GetConnectionCount != 1) {
				$self->debug("Dropping duplicate connection with $ip");
				$self->KillClient($client);
				$msg .= " -> not.. duplicate";
			}
			
		}
		else {
			$msg .= " not established: torrent_minpeers reached";
		}
	}
	else {
		$self->warn("Invalid call: $msg");
	}
	return({CHAINSTOP=>1,MSG=>[[1, $msg]]});
}


##########################################################################
# Callback : Accept new incoming connection
sub _Network_Accept {
	my($self, $sock) = @_;
	$self->debug("New incoming connection <$sock>");
	my $client = $self->Peer->AddNewClient($sock, {Ipv4 => $sock->peerhost, Port => 0});
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
	
	if(defined($client->GetSha1)) {
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
	my($self,$sock,$buffref,$len) = @_;
	
	my $RUNIT  = 1;
	my $client = $self->Peer->GetClient($sock) or $self->panic("Cannot handle non-existing client for sock <$sock>");
	$client->AppendReadBuffer($buffref,$len); # Append new data to client's full buffer
	
	
	while($RUNIT == 1) {
		my $status     = $client->GetStatus;
		my($bref,$len) = $client->GetReadBuffer;
		
		return if $len < BTMSGLEN;
		
		if(($status == STATE_READ_HANDSHAKE or $status == STATE_READ_HANDSHAKERES) && $len >= 68) {
			$self->debug("-> Reading handshake from peer");
			my $hs = $self->ParseHandshake($bref,$len);
			$client->DropReadBuffer(68); # Remove 68 bytes (Handshake) from buffer
			if(defined($hs->{sha1}) && $self->Torrent->GetTorrent($hs->{sha1}) && $hs->{peerid} ne $self->{CurrentPeerId}) {
				if($self->{super}->Queue->GetStats($hs->{sha1})->{clients} >= $self->{super}->Configuration->GetValue('torrent_maxpeers')) {
					$self->debug("<$client> $hs->{sha1} has reached torrent_maxpeers ; dropping new connection");
					$self->KillClient($client);
					return; # Go away!
				}
				else {
					$client->SetSha1($hs->{sha1}) unless defined($client->GetSha1); # Was unglued
					$client->SetExtensions(Kademlia=>$hs->{EXT_KAD}, FastPeers=>$hs->{EXT_FAST}, ExtProto=>$hs->{EXT_EPROTO});
					$client->SetRemotePeerID($hs->{peerid});
					$client->SetBitfield(pack("B*", ("0" x length(unpack("B*",$self->Torrent->GetTorrent($client->GetSha1)->GetBitfield)))));
					$client->SetStatus(STATE_IDLE);
					
					$client->WriteHandshake if $status == STATE_READ_HANDSHAKE; # Write our own handshake
					
					if($client->GetConnectionCount != 1) {
						$self->debug("Dropping duplicate, incoming connection from $client->{remote_ip}");
						$self->KillClient($client);
						return; # Go away
					}
					
					if($client->GetExtension('ExtProto')) {
						$client->WriteEprotoHandshake(Port=>$self->{super}->Configuration->GetValue('torrent_port'), Version=>'Bitflu '.BUILDID, UtorrentPex=>EP_UT_PEX);
					}
					
					$client->WriteBitfield; # ..and our bitfield
				}
			}
			else {
				$self->debug("<$client> failed to complete handshake");
				$self->KillClient($client);
				return; # Go away!
			}
		}
		else {
			my $msglen     = unpack("N", substr($$bref,0,BTMSGLEN));
			my $msgtype    = -1;
			   $msgtype    = unpack("c", substr($$bref,BTMSGLEN,1)) if $len > BTMSGLEN;
			my $payloadlen = BTMSGLEN+$msglen;
			my $readAT     = BTMSGLEN+1;
			my $readLN     = $payloadlen-$readAT;
			
			
			if($payloadlen > $len) { # Need to wait for more data
				return
			}
			else {
				## WARNING:: DO NEVER USE NEXT INSIDE THIS LOOP BECAUSE THIS WOULD SKIP DROPREADBUFFER
				
				if($msglen == 0) {
					$self->debug($client->XID." sent me a keepalive");
				}
				elsif($status == STATE_IDLE) {
					if($msgtype == MSG_PIECE) {
						my $this_piece = unpack("N",substr($$bref, $readAT, 4));
						my $this_offset= unpack("N",substr($$bref, $readAT+4,4));
						my $this_data  = substr($$bref, $readAT+8, $readLN-8);
						my $vrfy = $client->StoreData(Index=>$this_piece, Offset=>$this_offset, Size=>$readLN-8, Dataref=>\$this_data); # Kicks also Hunting
						$client->AdjustRanking(+1);
						$client->SetLastUsefulTime;
						$client->SetLastDownloadTime;
						
						if(defined($vrfy)) {
							$client->AdjustRanking(+3);
							$self->Torrent->GetTorrent($client->GetSha1)->SetHave($vrfy);
						}
						
						# ..and also update some stats:
						$self->Torrent->GetTorrent($client->GetSha1)->SetStatsDown($self->Torrent->GetTorrent($client->GetSha1)->GetStatsDown+$readLN);
					}
					elsif($msgtype == MSG_REQUEST) {
						my $this_piece = unpack("N",substr($$bref, $readAT, 4));
						my $this_offset= unpack("N",substr($$bref, $readAT+4,4));
						my $this_size  = unpack("N",substr($$bref, $readAT+8,4));
						$self->debug("Request { Index=> $this_piece , Offset => $this_offset , Size => $this_size }");
						$client->DeliverData(Index=>$this_piece, Offset=>$this_offset, Size=>$this_size) or return; # = DeliverData closed the connection
						$client->AdjustRanking(-1);
						$client->SetLastUsefulTime;
					}
					elsif($msgtype == MSG_HAVE_NONE) {
						$self->debug("Ignoring 'have none' message, did assume an empty bitfield");
					}
					elsif($msgtype == MSG_HAVE_ALL) {
						$client->SetBitfield(pack("B*", ("1" x length(unpack("B*",$self->Torrent->GetTorrent($client->GetSha1)->GetBitfield)))));
					}
					elsif($msgtype == MSG_EPROTO) {
						$self->debug("<$client> -> EPROTO");
						$client->ParseEprotoMSG(substr($$bref,$readAT,$readLN));
					}
					elsif($msgtype == MSG_CHOKE && !$client->GetChokeME) {
						$client->SetChokeME;
						$self->{super}->Queue->DecrementStats($client->GetSha1, {'active_clients' => 1});
						$self->debug($client->XID." -> Choked me");
					}
					elsif($msgtype == MSG_UNCHOKE && $client->GetChokeME) {
						$self->{super}->Queue->IncrementStats($client->GetSha1, {'active_clients' => 1});
						$self->debug($client->XID." -> Unchoked me");
						$client->SetUnchokeME;
						unless($client->GetInterestedME) { # Fixme: We'll be interested if we are a seeder!
							$client->WriteInterested;
						}
						$self->debug("<$client> -> Hunting!");
						$client->HuntPiece;
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
						my $have_piece = unpack("N",substr($$bref, $readAT, 4));
						$client->SetBit($have_piece);
						$client->TriggerHunt unless $self->Torrent->GetTorrent($client->GetSha1)->GetBit($have_piece);
						$self->debug("<$client> has piece: $have_piece");
					}
					elsif($msgtype == MSG_ALLOWED_FAST) {
						my $fast_allowed = unpack("N",substr($$bref,$readAT,4));
##						$self->warn("Allowed to FASTRQ $fast_allowed for ".$client->XID); # Fixme: unimplemented
					}
					elsif($msgtype == MSG_REJECT_REQUEST) {
						my $rejected = unpack("N",substr($$bref, $readAT, 4));
						if($client->GetPieceLocks->{$rejected}) {
							$client->ReleasePiece(Index=>$rejected);
						}
					}
					elsif($msgtype == MSG_CANCEL) {
						$self->debug("Ignoring cancel request because we do never queue-up REQUESTs.");
					}
					elsif($msgtype == MSG_BITFIELD) {
						$self->debug("<$client> -> BITFIELD");
						$client->SetBitfield(substr($$bref,$readAT,$readLN));
					}
					else {
						$self->debug($client->XID." Dropped Message: TYPE:$msgtype ;; MSGLEN: $msglen ;; LEN: $payloadlen ;; => unknown type or wrong state");
					}
				}
				# Drop the buffer
				$client->DropReadBuffer($payloadlen);
			}
		}
		
	}
	
	
}




sub ParseHandshake {
	my($self, $dataref, $datalen) = @_;
	my $ref = {peerid => undef, sha1 => undef, EXT_KAD => 0, EXT_FAST => 0, EXT_EPROTO => 0};
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
		$ref->{EXT_FAST}   = (substr($rawext, 61,1) == 1 ? 1 : 0);
		$ref->{EXT_EPROTO} = (substr($rawext, 43,1) == 1 ? 1 : 0);
	}
	else {
		$self->debug("ParseHandshake: Mumbled header: ($header) :: $hid");
	}
	return $ref;
}


sub KillClient {
	my($self, $client) = @_;
	$self->_Network_Close($client->GetOwnSocket);
	$self->{super}->Network->RemoveSocket($self, $client->GetOwnSocket);
	return undef;
}


sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub stop  { my($self, $msg) = @_; $self->{super}->stop(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }


package Bitflu::DownloadBitTorrent::Torrent;
	use strict;
	use constant SHALEN      => 20;
	use constant ALMOST_DONE => 25;
	use constant PPSIZE      => 8;
	
	##########################################################################
	# Returns a new Dispatcher Object
	sub new {
		my($class, %args) = @_;
		my $self = { super => $args{super}, _super => $args{_super}, Torrents => {} };
		$self->{super}->Admin->RegisterCommand('dumpbf',   $self, 'XXX_BitfieldDump', "Dumps BitTorrent bitfield (ADVANCED)");
		bless($self,$class);
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
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	

	
	
	##########################################################################
	# Register a .torrent file
	sub AddNewTorrent {
		my($self, %args) = @_;
		my $sid     = $args{StorageId} or $self->panic("No StorageId");
		my $torrent = $args{Torrent}   or $self->panic("No Torrent");
		my $sha1    = Digest::SHA1::sha1_hex(Bitflu::DownloadBitTorrent::Bencoding::encode(${$torrent}{info}));
		
		$sid =$self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open storage $sid");
		
		$self->panic("BUGBUG: Existing torrent! $sha1") if($self->{Torrents}->{$sha1});
		
		my $pieces = int(length(${$torrent}{info}->{pieces})/SHALEN);
		my $xo = { sha1=>$sha1, torrent=>$torrent, sid=>$sid, bitfield=>[], ppl=>[], super=>$self->{super}, Sockets=>{}, piecelocks=>{}, haves=>{}, private=>0 };
		bless($xo, ref($self));
		$self->{Torrents}->{$sha1} = $xo;
		
		
		my $bitfield = "0" x $pieces;
		   $bitfield = pack("B*",$bitfield);
		$xo->SetBitfield($bitfield);
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
		Carp::confess("No sha1?") unless $sha1;
		return $self->{Torrents}->{$sha1};
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
	
	
	
	sub TorrentwideLockPiece {
		my($self, $piece) = @_;
		if(++$self->{piecelocks}->{$piece} == 1) {
			# This was the first lock: lock it at storage level
			$self->Storage->SetAsInwork($piece);
		}
		return $self->TorrentwidePieceLockcount($piece);
	}
	
	sub TorrentwideReleasePiece {
		my($self, $piece) = @_;
		if(--$self->{piecelocks}->{$piece} == 0) {
			# Last lock released
			$self->Storage->SetAsFree($piece);
		}
		elsif($self->{piecelocks}->{$piece} < 0) {
			$self->panic("PieceLock for $piece fluked!");
		}
		return $self->TorrentwidePieceLockcount($piece);
	}
	
	sub TorrentwidePieceLockcount {
		my($self,$piece) = @_;
		return $self->{piecelocks}->{$piece};
	}
	
	
	
	sub Storage {
		my($self) = @_;
		return $self->{sid};
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
	# Set a new bitfield for this client
	sub SetBitfield {
		my($self, $bitfield) = @_;
		my $i = 0;
		foreach(split(//,$bitfield)) {
			$self->{bitfield}->[$i++] = $_;
		}
	}

	##########################################################################
	# Get current bitfield
	sub GetBitfield {
		my($self) = @_;
		my $buff = undef;
		foreach my $x (@{$self->{bitfield}}) { $buff .= $x; }
		return $buff;
	}
	
	##########################################################################
	# Set a single bit withing clients bitfield
	sub SetBit {
		my($self,$bitnum) = @_;
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		$self->{bitfield}->[$bfIndex] |= pack("C", 1<<7-$bitnum);
	}
	
	
	##########################################################################
	# Get a single bit from clients bitfield
	sub GetBit {
		my($self,$bitnum) = @_;
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		return (substr(unpack("B*",$self->{bitfield}->[$bfIndex]), $bitnum,1));
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
	sub IsAlmostComplete {
		my($self) = @_;
		my $stats = $self->{super}->Queue->GetStats($self->GetSha1);
		return( ($stats->{total_chunks} - $stats->{done_chunks}) < ALMOST_DONE ? 1 : 0 );
	}
	
	
	sub GenPPList {
		my($self) = @_;
		$self->{ppl} = [];
		my $skew      = 3;
		my $piecenum  = $self->Storage->GetSetting('chunks');
		for (0..PPSIZE) {
			my $rand = int(rand($piecenum));
			foreach my $ppitem ($rand..($rand+$skew)) {
				next if $self->GetBit($ppitem);
				next if $ppitem >= $piecenum;
				push(@{$self->{ppl}},$ppitem);
			}
		}
	}
	
	sub GetPPList {
		my($self) = @_;
		return $self->{ppl};
	}
	
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
	
1;


####################################################################################################################################################
package Bitflu::DownloadBitTorrent::Peer;
	use strict;
	use constant MSG_CHOKE          => 0;
	use constant MSG_UNCHOKE        => 1;
	use constant MSG_INTERESTED     => 2;
	use constant MSG_UNINTERESTED   => 3;
	use constant MSG_HAVE           => 4;
	use constant MSG_BITFIELD       => 5;
	use constant MSG_REQUEST        => 6;
	use constant MSG_PIECE          => 7;
	use constant MSG_EPROTO         => 20;
	use constant EP_UT_PEX          => 1;

	use constant PIECESIZE                => (2**14);
	use constant MAX_OUTSTANDING_REQUESTS => 32; # Upper for outstanding requests
	use constant MIN_OUTSTANDING_REQUESTS => 1;
	use constant DEF_OUTSTANDING_REQUESTS => 3;  # Default we are assuming
	use constant SHALEN                   => 20;
	
	
	##########################################################################
	# Register new dispatcher
	sub new {
		my($class, %args) = @_;
		my $self = { super=>$args{super}, _super=>$args{_super}, Sockets => {}, IPlist => {} };
		bless($self,$class);
		$self->{super}->Admin->RegisterCommand('x', $self, 'xxx_dump_peers', "DEBUG PEER");
		return $self;
	}
	
	

	sub xxx_dump_peers {
		my($self, @args) = @_;
		
		my $filter = $args[0];
		
		my @A = ();
		push(@A, [undef, sprintf("%-20s | %-20s | %-40s | ciCI | pieces | state | rank |lastused| rqmap", 'peerID', 'IP', 'Hash')]);
		
		my $peer_unchoked = 0;
		my $me_unchoked   = 0;
		foreach my $sock (keys(%{$self->{Sockets}})) {
			my $bitsux = unpack("B*",$self->{Sockets}->{$sock}->GetBitfield);
			   $bitsux =~ tr/1//cd;
				 $bitsux = length($bitsux);
			my $xpid = $self->{Sockets}->{$sock}->{remote_peerid};
			   $xpid =~ tr/a-zA-Z0-9_-//cd;
			my $rqm  = join(';', keys(%{$self->{Sockets}->{$sock}->{rqmap}}));
			
			next if (defined($filter) && $self->{Sockets}->{$sock}->{sha1} !~ /$filter/);
			
			$peer_unchoked++ unless $self->{Sockets}->{$sock}->{PEER_choked};
			$me_unchoked++   unless $self->{Sockets}->{$sock}->{ME_choked};
			
			push(@A, [undef, sprintf("%-20s | %-20s | %-40s | %s%s%s%s | %6d | %5d | %3d  | %6d | %s",$xpid,
			   $self->{Sockets}->{$sock}->{remote_ip},
			   $self->{Sockets}->{$sock}->{sha1},
			   $self->{Sockets}->{$sock}->{ME_choked},
			   $self->{Sockets}->{$sock}->{ME_interested},
			   $self->{Sockets}->{$sock}->{PEER_choked},
			   $self->{Sockets}->{$sock}->{PEER_interested},
			   $bitsux,
				 $self->{Sockets}->{$sock}->{status},
				 $self->{Sockets}->{$sock}->{ranking},
				 ($self->{super}->Network->GetTime - $self->{Sockets}->{$sock}->GetLastUsefulTime),
				 $rqm,
				 )]);
		}
		push(@A, [4, "Uploading to     $peer_unchoked peer(s)"]);
		push(@A, [4, "Downloading from $me_unchoked peer(s)"]);
		return({CHAINSTOP=>1, MSG=>\@A});
	}
	
	##########################################################################
	# Register a new TCP client
	sub AddNewClient {
		my($self, $socket, $args) = @_;
		$self->panic("BUGBUG: Duplicate socket: <$socket>") if defined($self->{Sockets}->{$socket});
		
		my $peer_id = $self->{_super}->{CurrentPeerId};
		
		
		my $xo = { socket=>$socket, main=>$self, super=>$self->{super}, _super=>$self->{_super}, local_peerid=>$peer_id,
		           remote_peerid => undef, remote_ip => $args->{Ipv4}, remote_port => 0, last_hunt => 0,
		           ME_interested => 0, PEER_interested => 0, ME_choked => 1, PEER_choked => 1, ranking => 0, rqslots => 0,
		           bitfield => undef, rqmap => {}, piececache => [], time_lastuseful => 0 , time_lastdownload => 0, extensions=>{}};
		bless($xo,ref($self));
		
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
		my $xpd = $self->{remote_peerid};
		   $xpd =~ tr/a-zA-Z0-9_-//cd;
		return "<$xpd|$self->{remote_ip}:$self->{remote_port}\@$self->{sha1}\[$self\]>";
	}
	
	sub SetRemotePort {
		my($self,$port) = @_;
		return $self->{remote_port} = int($port);
	}
	
	
	# WE got unchoked
	sub SetUnchokeME {
		my($self) = @_;
		return $self->{ME_choked} = 0;
	}
	
	# WE got choked
	sub SetChokeME {
		my($self) = @_;
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
		my $sha1 = $self->GetSha1 or $self->panic("No sha1 for $self ($self->{remote_ip})");
		return $self->{main}->{IPlist}->{$self->{remote_ip}}->{$sha1};
	}
	
	sub AdjustRanking {
		my($self,$rank) = @_;
		return ($self->{ranking} += $rank);
	}
	
	sub GetRanking {
		my($self) = @_;
		return $self->{ranking};
	}
	
	sub GetLastHunt {
		my($self) = @_;
		return $self->{last_hunt};
	}
	
	sub TriggerHunt {
		my($self) = @_;
		print "======= HUNT TRIGGERED ========\n";
		$self->{last_hunt} = 0;
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
		my($self) = @_;
		$self->{time_lastuseful} = $self->{super}->Network->GetTime;
	}
	
	sub SetLastDownloadTime {
		my($self) = @_;
		$self->{time_lastdownload} = $self->{super}->Network->GetTime;
	}
	sub GetLastDownloadTime {
		my($self) = @_;
		return $self->{time_lastdownload};
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
	
	sub LockPiece {
		my($self, %args) = @_;
		$self->{_super}->Torrent->GetTorrent($self->GetSha1)->TorrentwideLockPiece($args{Index});
		$self->panic("Duplicate lock : $args{Index}") if defined($self->{rqmap}->{$args{Index}});
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
	
	sub GetTotalPieceSize {
		my($self, $piece) = @_;
		my $torrent = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		my $pieces = $torrent->Storage->GetSetting('chunks');
		my $size   = $torrent->Storage->GetSetting('size');
		if($pieces == (1+$piece)) {
			# -> LAST piece
			$size -= $torrent->Storage->GetSetting('overshoot');
		}
		return $size;
	}
	
	
	
	sub HuntPiece {
		my($self, @suggested) = @_;
		
		# Idee: klassifikation
		# (gute) clients werden häufiger gehunted als phöse
		my $torrent      = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		return if $torrent->IsComplete; # Do not hunt complete torrents
		
		my $av_slots         = ($self->GetRequestSlots - int(keys(%{$self->GetPieceLocks})));
		my $piecenum         = $torrent->Storage->GetSetting('chunks');
		my @piececache       = @{$self->{piececache}};
		my @pplist           = @{$torrent->GetPPList};
		my %rqcache          = ();
		$self->{last_hunt}   = $self->{super}->Network->GetTime;
		
		if(@suggested) {
			# AutoSuggest 'near' pieces
			push(@suggested, $suggested[0]+1) if (($piecenum-2) > $suggested[0]);
			push(@suggested, $suggested[0]-1) if $suggested[0] > 0;
		}
		
		
		foreach my $slot (1..$av_slots) {
			my @xrand = ();
			for(0..16) { push(@xrand,int(rand($piecenum))); }
			foreach my $piece (@suggested, @piececache, @pplist, @xrand) {
				next unless $self->GetBit($piece);                       # Client does not have this piece
				next if     $torrent->TorrentwidePieceLockcount($piece); # Piece locked by other reference or downloaded
				next if     $rqcache{$piece};                            # Piece is about to get reqeuested
				next if     $torrent->GetBit($piece);                    # Got this anyway...
				my $this_offset = $torrent->Storage->GetSizeOfFreePiece($piece);
				my $this_size   = $self->GetTotalPieceSize($piece);
				my $bytes_left  = $this_size - $this_offset;
				   $bytes_left  = PIECESIZE if $bytes_left > PIECESIZE;
				$self->panic("FULL PIECE: $piece") if $bytes_left < 1;
				$rqcache{$piece} = {Index=>$piece, Size=>$bytes_left, Offset=>$this_offset};
				last;
			}
		}
		
		# Randomize did not find much stuff, do a slow search...
		if(int(keys(%rqcache)) < $av_slots) {
			foreach my $xpiece (0..($piecenum-1)) {
				if(!($rqcache{$xpiece}) && $self->GetBit($xpiece) && !($torrent->GetBit($xpiece)) && !($torrent->TorrentwidePieceLockcount($xpiece))) {
					my $this_offset = $torrent->Storage->GetSizeOfFreePiece($xpiece);
					my $this_size   = $self->GetTotalPieceSize($xpiece);
					my $bytes_left  = $this_size - $this_offset;
					   $bytes_left  = PIECESIZE if $bytes_left > PIECESIZE;
					$self->panic("FULL PIECE: $xpiece") if $bytes_left < 1;
					$rqcache{$xpiece} = {Index=>$xpiece, Size=>$bytes_left, Offset=>$this_offset};
					# Idee: fixme: wenn wir das rqcache nicht ganz füllen können, sollten wir das ding flaggen
					last unless int(keys(%rqcache)) < $av_slots;
				}
			}
		}
		
		# Update the piececache (even if empty)
		$self->{piececache} = [keys(%rqcache)];
		
		
		if(int(keys(%rqcache))) {
			# Client got interesting pieces!
			if($self->GetInterestedME) {
				if($self->GetChokeME) {
				}
				else {
					foreach my $xk (keys(%rqcache)) {
						$self->WriteRequest(%{$rqcache{$xk}});
						$self->SetLastDownloadTime;
					}
					$self->{piececache} = [];
				}
			}
			else {
				$self->WriteInterested;
			}
		}
		elsif(keys(%{$self->GetPieceLocks})) {
			# Still waiting for data.. hmm.. we should check how long we are waiting and KILL him
			# (..or maybe we shouldn't do it here because ->hunt may never be called again for this peer)
		}
		elsif($self->GetInterestedME) {
			$self->WriteUninterested;
		}
	}
	
	
	
	sub DeliverData {
		my($self, %args) = @_;
		
		my $good_client  = 1;
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
			
			$good_client = $self->WritePiece(%args);
		}
		else {
			$self->info("<$self> : Asked me for unadvertised data!");
			$good_client = 0;
		}
		
		unless($good_client) {
			$self->info("<$self> Droppin connection, write failed ($!)");
			$self->{_super}->KillClient($self);
		}
		return $good_client;
	}
	
	
	
	sub StoreData {
		my($self, %args) = @_;

		my $piece_is_storeable  = 0;
		my $piece_verified      = undef;
		my $torrent             = $self->{_super}->Torrent->GetTorrent($self->GetSha1);
		my $orq                 = $self->{rqmap}->{$args{Index}};
		my $pseudo_lock         = 0;
		
		if($orq->{Index}  == $args{Index}  && $orq->{Size} == $args{Size} &&
		   $orq->{Offset} == $args{Offset} && $torrent->Storage->GetSizeOfInworkPiece($args{Index}) == $orq->{Offset}) {
			# -> Response matches cached query
			$piece_is_storeable  = 1;
#			$self->info("[UXI] Storing data from trusted source : $args{Index}\@".$self->GetSha1);
		}
		elsif($torrent->IsAlmostComplete) {
			if($torrent->Storage->IsSetAsFree($args{Index})) {
				if($torrent->Storage->GetSizeOfFreePiece($args{Index}) == $args{Offset}) {
					$torrent->Storage->SetAsInwork($args{Index});
					$pseudo_lock        = 1;
					$piece_is_storeable = 1;
					$self->warn("[UXI] Using free piece $args{Index} to store old (untrusted) data \@".$self->GetSha1);
				}
				else {
					$self->warn("[UXI] Cannot store data for $args{Index} ; Piece if free but offset differs \@".$self->GetSha1);
				}
			}
			elsif($torrent->Storage->IsSetAsInwork($args{Index})) {
				my $xxxsiz = $torrent->Storage->GetSizeOfInworkPiece($args{Index});
				$self->warn("[UXI] DATA for inwork piece: $xxxsiz -> $args{Offset} \@ $args{Index}\@".$self->GetSha1);
				# Fixme: könnten wir nicht einfach die daten schreiben und nichts unlocken?
				# beim nächsten überschreiben würde ja von HuntPiece wieder das korrekte Offset genommen?!
				# Fixme: ---===> Was passiert, wenn das piece so voll wird und wir migrieren? Was passiert bei requests mit einer size von 0?
			}
		}
		else {
			$self->warn("[UXI] Ignoring data for $args{Index}\@".$self->GetSha1);
		}
			

		
		if($piece_is_storeable) {
			my $piece_fullsize = $self->GetTotalPieceSize($args{Index});
			my $piece_nowsize  = $torrent->Storage->WriteData(Chunk=>$args{Index}, Offset=>$args{Offset}, Length=>$args{Size}, Data=>$args{Dataref});
			
			
			if($pseudo_lock) { $torrent->Storage->SetAsFree($args{Index}) }
			else             { $self->ReleasePiece(%args);                }
			
			if($piece_fullsize == $piece_nowsize) {
				# Piece is completed: HashCheck it
				$torrent->Storage->SetAsInwork($args{Index});
				if(!$self->VerifyOk(Torrent=>$torrent, Index=>$args{Index}, Size=>$piece_nowsize)) {
					$self->warn("Verification of $args{Index}\@".$self->GetSha1." failed, starting ROLLBACK");
					$torrent->Storage->Truncate($args{Index});
					$torrent->Storage->SetAsFree($args{Index});
					$piece_verified = -1;
				}
				elsif($piece_nowsize != $piece_fullsize) {
					$self->panic("$args{Index} grew too much! $piece_nowsize != $piece_fullsize");
				}
				else {
					$self->debug("Verification of $args{Index}\@".$self->GetSha1." OK: Committing piece.");
					$torrent->SetBit($args{Index});
					$torrent->Storage->SetAsDone($args{Index});
					my $qstats = $self->{super}->Queue->GetStats($self->GetSha1);
					$self->{super}->Queue->SetStats($self->GetSha1, {done_bytes => $qstats->{done_bytes}+$piece_fullsize, done_chunks=>1+$qstats->{done_chunks}});
					$piece_verified = $args{Index};
				}
			}
			$self->HuntPiece($args{Index}) if $pseudo_lock == 0 && !$args{DisableHunt}
		}
		
		return $piece_verified;
	}
	
	##########################################################################
	# Returns 1 if piece is verified
	sub VerifyOk {
		my($self, %args) = @_;
		my $torrent   = $args{Torrent};
		my $piece     = $args{Index};
		my $this_size = $args{Size};
		my $sha1_file = Digest::SHA1::sha1($torrent->Storage->ReadInworkData(Chunk=>$piece, Offset=>0, Length=>$this_size));
		my $sha1_trnt = substr($torrent->{torrent}->{info}->{pieces}, ($piece*SHALEN), SHALEN);
		return 1 if $sha1_file eq $sha1_trnt;
		return 0;
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
			my $val = $args{$k};
			if($val == 0) {
				delete($self->{extensions}->{$k});
			}
			else {
				$self->{extensions}->{$k} = $val;
			}
		}
	}
	
	##########################################################################
	#
	sub GetExtension {
		my($self,$key) = @_;
		return $self->{extensions}->{$key};
	}
	
	sub ParseEprotoMSG {
		my($self,$string) = @_;
		
		my $etype     = unpack("c",substr($string,0,1));
		my $bencoded  = substr($string,1);
		my $decoded   = Bitflu::DownloadBitTorrent::Bencoding::decode($bencoded);
		
		
		if($etype == 0) {
			foreach my $ext_name (keys(%{$decoded->{m}})) {
				if($ext_name eq "ut_pex") {
					$self->SetExtensions(UtorrentPex=>$decoded->{m}->{$ext_name});
				}
				else {
					$self->warn($self->XID." Unknown eproto extension '$ext_name' (id: $decoded->{m}->{$ext_name})");
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
		elsif($etype == EP_UT_PEX) {
			my $compact_list = $decoded->{added};
			my $nnodes = 0;
			for(my $i=0;$i<length($compact_list);$i+=6) {
				my $chunk = substr($compact_list, $i, 6);
				my $a    = unpack("C", substr($chunk,0,1));
				my $b    = unpack("C", substr($chunk,1,1));
				my $c    = unpack("C", substr($chunk,2,1));
				my $d    = unpack("C", substr($chunk,3,1));
				my $port = unpack("n", substr($chunk,4,2));
				my $ip = "$a.$b.$c.$d";
				$nnodes++;
				$self->{_super}->CreateNewOutgoingConnection($self->GetSha1, $ip, $port);
			}
			$self->debug($self->XID." $nnodes new nodes via ut_pex (\$etype == $etype)");
		}
		else {
			$self->warn($self->XID." Ignoring message for unregistered/unsupported id: $etype");
		}
		
	}
	
	##########################################################################
	# 'Link' a SHA1 to this client
	sub SetSha1 {
		my($self,$sha1) = @_;
		my $sx = $self->{main}->{Sockets}->{$self->{socket}} or $self->panic("Stale socket: $self->{socket}");
		$self->panic("this client had it's own sha1 set!") if defined($self->{sha1});
		$self->{sha1} = $sha1;
		$self->{_super}->Torrent->LinkTorrentToSocket($sha1,$self->GetOwnSocket);
		$self->{super}->Queue->IncrementStats($sha1, {'clients' => 1});
		$self->{main}->{IPlist}->{$self->{remote_ip}}->{$sha1}++;
	}
	
	##########################################################################
	# Delink SHA1 from this client
	sub UnsetSha1 {
		my($self) = @_;
		my $sha1 = $self->GetSha1 or return undef;  # Sha1 was not registered
		
		$self->{_super}->Torrent->UnlinkTorrentToSocket($sha1, $self->GetOwnSocket);
		
		my $refcount = --$self->{main}->{IPlist}->{$self->{remote_ip}}->{$sha1};
		
		if($refcount == 0) {
			delete($self->{main}->{IPlist}->{$self->{remote_ip}}->{$sha1}); # Free memory
			if(int(keys(%{$self->{main}->{IPlist}->{$self->{remote_ip}}})) == 0) {
				delete($self->{main}->{IPlist}->{$self->{remote_ip}});
				$self->debug("$self->{remote_ip} lost all connections");
			}
		}
		if($refcount < 0) {
			$self->panic("Refcount mismatch for $self->{remote_ip}\@$sha1 : $refcount");
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
	# Set a new bitfield for this client
	sub SetBitfield {
		my($self, $bitfield) = @_;
		my $i = 0;
		foreach(split(//,$bitfield)) {
			$self->{bitfield}->[$i++] = $_;
		}
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
	# Get current bitfield
	sub GetBitfield {
		my($self) = @_;
		my $buff = undef;
		foreach my $x (@{$self->{bitfield}}) { $buff .= $x; }
		return $buff;
	}
	
	##########################################################################
	# Set a single bit withing clients bitfield
	sub SetBit {
		my($self,$bitnum) = @_;
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		$self->{bitfield}->[$bfIndex] |= pack("C", 1<<7-$bitnum);
	}
	
	##########################################################################
	# Get a single bit from clients bitfield
	sub GetBit {
		my($self,$bitnum) = @_;
		my $bfIndex = int($bitnum / 8);
		$bitnum -= 8*$bfIndex;
		return (substr(unpack("B*",$self->{bitfield}->[$bfIndex]), $bitnum,1));
	}
	
	##########################################################################
	# Buffer for unfinished data
	sub AppendReadBuffer {
		my($self,$buffref, $bufflen) = @_;
		$self->{read_buff}  .= ${$buffref};
		$self->{read_buffL} += $bufflen;
		return 1;
	}
	
	##########################################################################
	# Clean read buffer
	sub DropReadBuffer {
		my($self, $bytes) = @_;
		if($bytes < 0) {
			# Drop everything
			$self->{read_buff} = '';
			$self->{read_buffL}= 0;
		}
		else {
			$self->{read_buff} = substr($self->{read_buff},$bytes);
			$self->{read_buffL}-=$bytes;
		}
		$self->panic() if $self->{read_buffL} < 0;
	}
	
	##########################################################################
	# Returns the current read buffer
	sub GetReadBuffer {
		my($self) = @_;
		return(\$self->{read_buff},$self->{read_buffL});
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
		   $buff.= $self->{local_peerid};
		$self->debug("$self : Wrote Handshake");
		return $self->{super}->Network->WriteData($self->{socket},$buff);
	}
	
	sub WriteEprotoHandshake {
		my($self, %args) = @_;
		my $xh = Bitflu::DownloadBitTorrent::Bencoding::encode({ reqq => MAX_OUTSTANDING_REQUESTS, e=>0, v=>$args{Version}, p=>$args{Port}, m => { ut_pex => int($args{UtorrentPex}) } });
		my $buff =  pack("N", 2+length($xh));
		   $buff .= pack("c", MSG_EPROTO).pack("c", 0).$xh;
		return $self->{super}->Network->WriteData($self->{socket},$buff);
	}
	
	##########################################################################
	# Write our current bitfield to this client
	sub WriteBitfield {
		my($self) = @_;
		
		my $tobj = $self->{_super}->Torrent->GetTorrent($self->{sha1}) or $self->panic("No torrent!");
		my $bitfield = $tobj->GetBitfield;
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
		$self->debug("$self : Wrote PING");
		return $self->{super}->Network->WriteData($self->{socket}, pack("cccc",0000));
	}
	

	sub WriteInterested {
		my($self) = @_;
		$self->SetInterestedME;
		$self->debug("$self : Wrote INTERESTED");
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_INTERESTED));
	}

	sub WriteUninterested {
		my($self) = @_;
		$self->SetUninterestedME;
		$self->debug("$self : Wrote -UN-INTERESTED");
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_UNINTERESTED));
	}
	
	sub WriteUnchoke {
		my($self) = @_;
		$self->panic("Cannot UNchoke an unchoked peer") if !$self->GetChokePEER;
		$self->SetUnchokePEER;
		$self->debug("$self : Unchoked peer");
		return $self->{super}->Network->WriteData($self->{socket}, pack("N",1).pack("c", MSG_UNCHOKE));
	}
	
	sub WriteChoke {
		my($self) = @_;
		$self->panic("Cannot choke a choked peer") if $self->GetChokePEER;
		$self->SetChokePEER;
		$self->debug("$self : Choked peer");
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
		$self->debug("$self : Delivering to client: Index=>$args{Index}");
		return $self->{super}->Network->WriteData($self->{socket}, $x);
	}
	
	sub WriteEprotoMessage {
		my($self, %args) = @_;
		$args{Index} or $self->panic("No index!");
		my $encoded = Bitflu::DownloadBitTorrent::Bencoding::encode($args{Message});
		my $x .= pack("N", 2+length($encoded));
		   $x .= pack("c", MSG_EPROTO);
		   $x .= pack("C", int($args{Index}));
		   $x .= $encoded;
		return $self->{super}->Network->WriteData($self->{socket},$x);
	}
	
	sub WriteRequest {
		my($self, %args) = @_;
		my $x = pack("N",13);
		$x .= pack("c", MSG_REQUEST);
		$x .= pack("N", $args{Index});
		$x .= pack("N", $args{Offset});
		$x .= pack("N", $args{Size});
		
		$self->panic("EMPTY SIZE?!") if $args{Size} == 0;
		
		$self->LockPiece(Index=>$args{Index}, Offset=>$args{Offset}, Size=>$args{Size});
		$self->debug($self->XID." : Request { Index => $args{Index} , Offset => $args{Offset} , Size => $args{Size} }");
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
			#Enables FastProto
			substr($ext,61,1,1);
		}
		if(0) {
			# We do never advertise DHT, all we'd get are stupid PORT commands
			substr($ext,63,1,1);
		}
		return pack("B64",$ext);
	}
	
	sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
	sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
	sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
	sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }
	
1;



##################################################################################################################################
package Bitflu::DownloadBitTorrent::Bencoding;


	sub decode {
		my($string) = @_;
		Carp::confess("decode(undef) called") unless $string;
		my @chars = split(//,$string);
		return _decode(\@chars);
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
	
	
	sub _decode {
		my($ref) = @_;
		
		my $cx = shift(@$ref);
		if($cx eq 'd') {
			# -> A Dict : Cycle-Decode chunks until we get to an 'e'
			my $stolen_item = shift(@{$ref});
			my %ret_hash = ();
			while(defined($stolen_item) && $stolen_item ne 'e') {
				unshift(@{$ref},$stolen_item);
				my $key = _decode($ref);
				my $val = _decode($ref);
				$ret_hash{$key} = $val;
				$stolen_item = shift(@{$ref}); #NextOne
			}
			return \%ret_hash;
		}
		elsif($cx =~ /^\d$/) {
			# -> LengthPrefixed string
			my $string = undef;
			my $length = $cx;
			
			my $stolen_item = shift(@{$ref});
			while(defined($stolen_item) && $stolen_item ne ':') {
				$length .= $stolen_item;
				$stolen_item = shift(@{$ref});
			}
			for(1..int($length)) {
				$string .= shift(@{$ref});
			}
			return $string;
		}
		elsif($cx =~ 'i') {
			# -> An int, cycle until we find a non-int (e)
			my $fullnum = undef;
			while(defined (my $append = shift(@{$ref}))) {
				last if $append !~ /^\d$/;
				
				$fullnum .= $append;
			}
			return $fullnum;
		}
		elsif($cx eq 'l') {
			# -> A List (aka Array) ; Cycle-Decode until we got an 'e'
			my $stolen_item = shift(@{$ref});
			my @ret_array = ();
			while(defined($stolen_item) && $stolen_item ne 'e') {
				unshift(@{$ref}, $stolen_item);
				push(@ret_array, _decode($ref));
				$stolen_item = shift(@{$ref});
			}
			return \@ret_array;
		}
		elsif(defined($cx)) {
#			warn "$0::Bitflu::Bencoding : Unexpected entry '$cx' found; Aborting parsing due to syntax error.\n";
			return undef;
		}
	}

#################################################################
# Load a torrent file
sub torrent2hash {
	my($file) = @_;
	my $buff = undef;
	open(BENC, "<", $file) or return {};
	while(<BENC>) {
		$buff .= $_;
	}
	close(BENC);
	return {} unless $buff;
	return data2hash($buff);
}

sub data2hash {
	my($buff) = @_;
	my $href = decode($buff);
	return {} unless ref($href) eq "HASH";
	my $torrent_hash =  Digest::SHA1::sha1_hex(encode(${$href}{info}));
	return {torrent_hash=>$torrent_hash, content=>$href, torrent_data=>$buff};	
}



1;
