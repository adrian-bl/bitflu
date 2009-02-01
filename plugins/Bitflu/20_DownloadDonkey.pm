package Bitflu::DownloadDonkey;
#
# This file is part of 'Bitflu' - (C) 2008-2009 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use constant _BITFLU_APIVERSION => 20090202;
use POSIX;
use List::Util;
use Data::Dumper;

use constant CHUNKSIZE         => 9728000;
use constant MAX_SERVERS       => 3;
use constant CLIPBOARD_PREFIX  => 'eDonkey';

use constant STATE_AWAIT_IHELLO => 100; # Incoming hello
use constant STATE_AWAIT_RHELLO => 101; # Remote hello
use constant STATE_IDLE         => 200;

sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, Dispatch => { Peers => undef }, eservers=>{} };
	bless($self,$class);
	
	return $self; # disabled for now
	
	$self->{Dispatch}->{Peers}    = Bitflu::DownloadDonkey::Peers->new(super=>$mainclass, _super=>$self);
	$mainclass->AddRunner($self) or $self->panic();
	
	my $main_socket = $mainclass->Network->NewTcpListen(ID=>$self, Port=>4662,
	                                                    Bind=>0,
	                                                    MaxPeers=>900,
	                                                    Callbacks => {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	
	if($main_socket) {
		$mainclass->AddRunner($self);
	}
	else {
		$self->stop("FIXME: Unable to listen :$!");
	}
	
	return $self;
}


sub init {
	my($self) = @_;
	
	return 1; # disabled for now
	
	$self->{super}->Admin->RegisterCommand('eservers', $self, '_Command_Eservers', "List (and connect) to edonkey servers",
	[ [undef, "Usage: econnect ip port"],
	]);

	$self->{super}->Admin->RegisterCommand('econnect', $self, '_Command_Econnect', "Connect to an eDonkey peer",
	  [ [undef, "Usage: econnect md4 ip port"] ] );
	$self->{super}->Admin->RegisterCommand('search', $self, 'SearchDonkey', "Search for given string",
	[ [undef, "Usage: search string"] ]);
	$self->{super}->Admin->RegisterCommand('esources', $self, 'SourceDonkey', "Request sources",
	[ [undef, "Usage: esources md4 size"] ]);
	$self->{super}->Admin->RegisterCommand('load', $self, '_Command_Load', "Start eDonkey download",
	  [ [undef, "Starts an eDonkey download"] ] );
	
	return 1;
}


sub run {
	my($self) = @_;
	$self->{super}->Network->Run($self);
	my $NOWTIME = $self->{super}->Network->GetTime;
	
	if($self->{lastrun}+5 < $NOWTIME) {
		$self->{lastrun} = $NOWTIME;
		$self->warn("..running...");
		
		my $peers = $self->Peers->GetPeers;
		
		foreach my $this_peer_name (keys(%$peers)) {
			$self->warn(" >> Checking state of $this_peer_name");
			my $peer = $self->Peers->GetPeer($this_peer_name);
			if($peer->HasMd4) {
				my $slotstate = $peer->Slot;
				$self->warn("  >> Bound to ".$peer->GetMd4." with slotstate of $slotstate");
				if($slotstate & chr(1)) {
					$self->warn(" >> Peer confirmed to have file");
				}
			}
			else {
				$self->warn("  >> Unbound");
			}
		}
		
	}
	
	
	return 0;
}


sub Peers {
	my($self, %args) = @_;
	$self->{Dispatch}->{Peers};
}

sub resume_this {
	my($self, $sid) = @_;
	$self->info("Resuming $sid");
	if(my $so = $self->{super}->Storage->OpenStorage($sid)) {
		my $total_bytes    = (($so->GetSetting('chunks')-1) * $so->GetSetting('size')) + ($so->GetSetting('size') - $so->GetSetting('overshoot'));
		$self->{super}->Queue->SetStats($sid, {total_bytes=>$total_bytes, done_bytes=>0, uploaded_bytes=>0,
		                                        active_clients=>0, clients=>0, speed_upload =>0, speed_download => 0,
		                                        last_recv => 0, total_chunks=>int($so->GetSetting('chunks')), done_chunks=>0});
		return $so;
	}
	else {
		$self->panic("Failed to open $sid");
	}
}

sub cancel_this {
	my($self, $sid) = @_;
	$self->warn("Not implemented yet");
	$self->{super}->Queue->RemoveItem($sid);
}


##########################################################################
# Load an edonkey link
sub _Command_Load {
	my($self, @args) = @_;
	
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	
	foreach my $this (@args) {
		if(my($desc,$size,$md4) = $this =~ /^ed2k:\/\/\|file\|([^|]+)\|(\d+)\|([a-fA-F0-9]{32})\|/i) {
			$md4 .= "0" x 8; # 'simulate' a sha1 sum :-)
			if($self->{super}->Storage->OpenStorage($md4)) {
				push(@MSG, [2, "$md4 : Download '$this' exists in queue"]);
			}
			else {
				my $numpieces = 1;
				my $piecelen  = $size;
				my $overshoot = 0;
				if($size > CHUNKSIZE) {
					$numpieces = POSIX::ceil($size/CHUNKSIZE);
					$piecelen  = CHUNKSIZE;
					$overshoot = ($numpieces*CHUNKSIZE)-$size;
				}
				
				my $so = $self->{super}->Queue->AddItem(Name=>$desc, Chunks=>$numpieces, Overshoot=>$overshoot,
				                                        Size=>$piecelen, Owner=>$self, ShaName=>$md4,
				                                        FileLayout=>{ $desc => { start => 0, end => $size, path=>[$desc] } });
				if($so) {
					$so->SetSetting('type', 'ed2k') or $self->panic("Unable to store type setting : $!");
					$self->resume_this($md4);
				}
				else {
					push(@MSG, [2, "$@"]);
				}
			}
		}
		elsif(my($ip,$port) = $this =~ /^ed2k:\/\/\|server\|(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\|(\d+)\|/i) {
			my $srvlist = $self->{super}->Tools->CBxToRef($self->{super}->Storage->ClipboardGet(CLIPBOARD_PREFIX));
			$srvlist->{$ip} = $port;
			$self->{super}->Storage->ClipboardSet(CLIPBOARD_PREFIX, $self->{super}->Tools->RefToCBx($srvlist));
			push(@MSG, [1, "Added eDonkey-Server $ip:$port"]);
		}
		else {
			push(@SCRAP, $this);
		}
	}
	if(!int(@args)) {
		$NOEXEC = 'Usage: load "ed2k://|file|Sample File|1234567890abcdef1234567890abcdef|"';
	}
	
	return({MSG=>\@MSG, SCRAP=>\@SCRAP, NOEXEC=>$NOEXEC});
}

##########################################################################
# List and connect to some edonkey servers
sub _Command_Eservers {
	my($self) = @_;
	
	my @MSG       = ();
	my $srvlist   = $self->{super}->Tools->CBxToRef($self->{super}->Storage->ClipboardGet(CLIPBOARD_PREFIX)); # Get internal serverlist
	my $connected = $self->{eservers};                                                                        # Sockname of all connected servers
	my $credits   = MAX_SERVERS;
	
	foreach my $peername (keys(%$connected)) {
		my $peer  = $self->Peers->GetPeer($peername);
		my $cip   = $peer->GetIp;
		my $stats = $peer->GetStats;
		push(@MSG, [undef, "Connected:  $cip:".$peer->GetPort], [1, "  >> Users: $stats->{users}, Files: $stats->{files}"]);
		delete($srvlist->{$cip}); # Delete from (current inmemory-copy of) serverlist
		$credits--;
	}
	
	
	foreach my $srv_ip (List::Util::shuffle(keys(%$srvlist))) {
		my $srv_port = $srvlist->{$srv_ip};
		push(@MSG, [undef, "Known   : $srv_ip:$srv_port"]);
		
		next if $credits-- <= 0;
		
		push(@MSG, [3, " >> Connecting..."]);
		
		my $sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$srv_port, Ipv4=>$srv_ip, Timeout=>5) or next;
		my $peer = $self->Peers->AddPeer(Socket=>$sock, Server=>1, Port=>$srv_port, Ipv4=>$srv_ip);
		$peer->WriteLoginMessage(Tags=>[]);
		$self->{eservers}->{$sock} = $sock; # Register as an eserver
	}
	
	return({MSG=>\@MSG, SCRAP=>[]});
}

sub _Command_Econnect {
	my($self, $md4, $ip, $port) = @_;
	my @MSG = ([1, "Connecting to edonkey://$md4/$ip/$port"]);
	
	my $sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$port, Ipv4=>$ip, Timeout=>15) or next;
	my $peer = $self->Peers->AddPeer(Socket=>$sock, Server=>0, Port=>$port, Ipv4=>$ip);
	$peer->SetMd4($md4);                 # 'glue' md4 to this connection
	$peer->WriteHello(Response=>0);      # Send initial hello
	$peer->Status(STATE_AWAIT_RHELLO);   # Expect a response
	
	return({MSG=>\@MSG, SCRAP=>[]});
}


sub SearchDonkey {
	my($self,$str) = @_;
	
	my @MSG = ();
	
	push(@MSG, [2, "Searching for '$str' ..."]);
	foreach my $pn (keys(%{$self->{eservers}})) {
		push(@MSG, [1, "Sent search request to eserver $pn"]);
		my $peer = $self->Peers->GetPeer($pn);
		$peer->WriteSearchRequest($str);
	}
	
	return({MSG=>\@MSG, SCRAP=>[]});
}


##########################################################################
# Obtain sources
sub SourceDonkey {
	my($self,$md4, $size) = @_;
	
	my @MSG = ();
	push(@MSG, [2, "Requesting sources for $md4 (Size=$size)"]);
	
	foreach my $pn (keys(%{$self->{eservers}})) {
		push(@MSG, [1, "Sent sources request to eserver $pn"]);
		my $peer = $self->Peers->GetPeer($pn);
		$peer->WriteGetSources($md4, $size);
	}
	return({MSG=>\@MSG, SCRAP=>[]});
}



sub _Network_Accept {
	my($self, $sock, $ip) = @_;
	$self->info("Incoming connection <$sock> from $ip");
	my $peer = $self->Peers->AddPeer(Socket=>$sock, Server=>0, Ipv4=>$ip);
	$peer->Status(STATE_AWAIT_IHELLO);
	
}

sub _Network_Data {
	my($self,$sock,$buffref,$blen) = @_;
	$self->info("<$sock> sent us $blen bytes");
	
	my $peer = $self->Peers->GetPeer($sock);
	my $srv  = $peer->IsServer;
	$peer->AppendData($$buffref);
	
	my $buffer = $peer->GetData;
	my $bufflen = length($buffer);
	
	while($bufflen) {
		my $rx = $peer->ParseMessage($buffer);
		
		warn "Offset: $rx->{offset} , blen=>$bufflen\n";
		
		return if $rx->{offset} < 0; # Incomplete data
		
		$bufflen -= $rx->{offset};
		$buffer   = substr($buffer, $rx->{offset});
		
		if($srv) { $peer->HandleServerMessage($rx) }
		else     { $peer->HandleClientMessage($rx) }
	}
	
	$peer->ClearData($buffer);
}

##########################################################################
# Close-Down eDonkey connection
sub _Network_Close {
	my($self, $sock) = @_;
	$self->info("<$sock> is gone");
	
	my $peer = $self->Peers->GetPeer($sock);
	if($peer->IsServer) {
		delete($self->{eservers}->{$sock}) or $self->panic("Cannot remove non-existing eserver $sock");
	}
	$self->Peers->KillPeer($sock);
}


sub debug { my($self, $msg) = @_; $self->{super}->debug("Donkey  : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Donkey  : ".$msg);  }
sub stop  { my($self, $msg) = @_; $self->{super}->stop("Donkey  : ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("Donkey  : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Donkey  : ".$msg); }
1;

package Bitflu::DownloadDonkey::Peers;
	use strict;
	use Compress::Zlib;
	
	use constant PROTO_VERSION    => 0xe3;
	use constant PROTO_HASZLIB    => 0xd4;
	
	use constant CP_HELLO         => 0x01;
	use constant CP_PARTSRQ       => 0x47;
	use constant CP_HELLOREPLY    => 0x4c;
	use constant CP_FILESTATUSRQ  => 0x4f;
	use constant CP_FILESTATUSRPLY => 0x50;
	use constant CP_HASHSETRQ     => 0x51;
	use constant CP_HASHSETREPLY  => 0x52;
	use constant CP_SLOTRQ        => 0x54;
	use constant CP_FILERQ        => 0x58;
	use constant CP_FILEREPLY     => 0x59;
	use constant OP_LOGINREQUEST  => 0x01;
	use constant OP_SEARCHREQ     => 0x16;
	use constant OP_GETSOURCES    => 0x19;
	use constant OP_SEARCHRES     => 0x33;
	use constant OP_SERVERINFO    => 0x34;
	use constant OP_SERVERMSG     => 0x38;
	use constant OP_IDCHANGE      => 0x40;
	use constant OP_FOUNDSOURCES  => 0x42;
	
	use constant TAG_NICKNAME     => 0x01;
	use constant TAG_PROTOVERS    => 0x11;
	use constant TAG_FLAGS        => 0x20;
	use constant TAG_TCPPORT      => 0x0f;
	
	use constant TAG_COMPCLIENT   => 0xef;
	use constant TAG_UDPPORT      => 0xf9;
	use constant TAG_EMULEMISC1   => 0xfa;
	use constant TAG_EMULEVERS    => 0xfb;
	use constant TAG_EMULEMISC2   => 0xfe;
	
	use constant FLAG_HAS_ZLIB    => 0x01;
	
	use constant SLOT_HASFILE      => 1;    # Set to true if peer has bound file
	use constant SLOT_ME_REQUESTED => 2;    # Set if we sent a slot request
	use constant SLOT_ME_GRANTED   => 4;    # Set if we received a slot
	use constant SLOT_HE_REQUESTED => 8;    # Set if peer requested an upload slot
	use constant SLOT_HE_GRANTED   => 16;   # Set if we gave the peer an upload slot
	
	
	sub new {
		my($class, %args) = @_;
		my $self = { super => $args{super}, _super => $args{_super}, peers => {} };
		bless($self,$class);
		return $self;
	}
	
	sub AddPeer {
		my($self,%args) = @_;
		my $sock = delete($args{Socket})    or $self->panic("No socket?");
		my $ip   = delete($args{Ipv4})      or $self->panic("No IP?!");
		my $port = delete($args{Port});
		my $srv  = ($args{Server} ? 1 : 0);
		
		if(exists($self->{peers}->{$sock})) {
			$self->panic("Peer named $sock did exist!");
		}
		my $xo = { super=>$self->{super}, _super=>$self->{_super}, socket=>$sock, is_server=>$srv, dbuff=>'', client_id=>0, port=>0, ipv4=>$ip, port=>$port,
		           status=>0, stats=>{users=>0, files=>0}, md4=>undef, slot_status=>chr(0),  };
		
		bless($xo, ref($self));
		$self->{peers}->{$sock} = $xo;
		return $xo;
	}
	
	sub KillPeer {
		my($self, $sock) = @_;
		delete($self->{peers}->{$sock}) or $self->panic("Cannot delete non-existing peer $sock");
	}
	
	sub GetPeer {
		my($self, $sockname) = @_;
		return $self->{peers}->{$sockname} or $self->panic("No such peer: $sockname");
	}
	
	sub GetPeers {
		my($self) = @_;
		return $self->{peers};
	}
	
	
	
	
	sub AppendData {
		my($pself, $data) = @_;
		$pself->{dbuff} .= $data;
	}
	
	sub ClearData {
		my($pself, $data) = @_;
		$data = '' if !defined($data);
		$pself->{dbuff} = $data;
	}
	
	sub GetData {
		my($pself) = @_;
		return $pself->{dbuff};
	}
	
	sub GetIp {
		my($pself) = @_;
		return $pself->{ipv4};
	}
	
	sub GetPort {
		my($pself) = @_;
		return $pself->{port};
	}
	
	sub GetStats {
		my($pself) = @_;
		return $pself->{stats};
	}
	
	sub GetMd4 {
		my($pself) = @_;
		return $pself->{md4} or $pself->panic("No md4 bound to $pself");
	}
	
	sub HasMd4 {
		my($pself) = @_;
		return ($pself->{md4} ? 1 : 0);
	}
	
	sub IsServer {
		my($pself) = @_;
		return $pself->{is_server};
	}
	
	
	sub SetPort {
		my($pself, $port) = @_;
		return $pself->{port} = $port;
	}
	
	sub SetStats {
		my($pself, %args) = @_;
		foreach my $k (keys(%args)) {
			$pself->{stats}->{$k} = $args{$k};
		}
	}
	
	sub SetMd4 {
		my($pself, $md4) = @_;
		return $pself->{md4} = $md4;
	}
	
	sub SetClientId {
		my($pself, $cid) = @_;
		return $pself->{client_id} = $cid;
	}
	
	sub Status {
		my($pself, $what) = @_;
		$pself->{status} = $what if $what;
		return $pself->{status};
	}
	
	sub Slot {
		my($pself,$bit,$val) = @_;
		if(defined($bit) && defined($val)) {
			vec($pself->{slot_status},$bit,1) = $val;
		}
		return $pself->{slot_status};
	}
	
	
	
	sub WriteSearchRequest {
		my($pself, $string) = @_;
		$pself->{super}->Network->WriteData($pself->{socket},
		                                  pack("C V C C v",PROTO_VERSION, 4+length($string), OP_SEARCHREQ, 0x01, length($string)).$string);
	}
	
	sub WriteGetSources {
		my($pself, $md4, $size) = @_;
		$pself->{super}->Network->WriteData($pself->{socket},
		                                  pack("C V C H32 V",PROTO_VERSION, 21, OP_GETSOURCES, $md4, $size));
	}
	
	sub WriteLoginMessage {
		my($pself, %args) = @_;
		
		my @tags     = @{$args{Tags}};
		
		unshift(@tags, $pself->CreateTag(EmuleVersion=>50304, Flags=> FLAG_HAS_ZLIB, Version=>62, Nickname=>'bitflu'));
		
		my $append = join('',@tags);
		my $buff = '';
		$buff .= pack("C", OP_LOGINREQUEST);
		$buff .= pack("H*","575e4afb720e736eb8b1eed28f9a6ff2"); # Fixme!
		$buff .= pack("V", 0);
		$buff .= pack("v", 4662);
		$buff .= pack("V", int(@tags));
		$buff .= $append;
		$pself->{super}->Network->WriteData($pself->{socket},
		                     pack("C V", PROTO_VERSION, length($buff)).$buff) or $pself->panic("Write failed");
	}
	
	sub WriteHello {
		my($pself, %args) = @_;
		my @tags = $pself->CreateTag(Version=>62, Nickname=>'bitflu', CompatibleClient=>1);
		my $buff  = pack("C", ($args{Response} ? CP_HELLOREPLY : CP_HELLO) );
		   $buff .= pack("C", 0x10) unless($args{Response}); # ?
		   $buff .= pack("H*","575e4afb720e736eb8b1eed28f9a6ff2");
		   $buff .= pack("V", 0); # FIXME: Client ID
		   $buff .= pack("v", 4662);
		   $buff .= pack("V", int(@tags));
		   $buff .= join('',@tags);
		   $buff .= pack("V v", 0,0); # ??
		  $pself->{super}->Network->WriteData($pself->{socket},
		                     pack("C V", PROTO_VERSION, length($buff)).$buff) or $pself->panic("Write failed");
	}
	
	sub WriteSlotRequest {
		my($pself) = @_;
		$pself->info(" $pself >> SlotRequest");
		$pself->Slot(SLOT_ME_REQUESTED,1);
		$pself->{super}->Network->WriteData($pself->{socket}, pack("C V C H32", PROTO_VERSION, 17, CP_SLOTRQ, $pself->GetMd4));
	}
	
	sub WriteFileRequest {
		my($pself) = @_;
		$pself->info(" $pself >> FileRequest");
		$pself->{super}->Network->WriteData($pself->{socket}, pack("C V C H32", PROTO_VERSION, 17, CP_FILERQ, $pself->GetMd4));
	}
	
	sub WriteHashsetRequest {
		my($pself) = @_;
		$pself->info(" $pself >> HashsetRequest");
		$pself->{super}->Network->WriteData($pself->{socket}, pack("C V C H32", PROTO_VERSION, 17, CP_HASHSETRQ, $pself->GetMd4));
	}
	
	sub WriteFileStatusRequest {
		my($pself) = @_;
		$pself->info(" $pself >> FileStatusRequest");
		$pself->{super}->Network->WriteData($pself->{socket}, pack("C V C H32", PROTO_VERSION, 17, CP_FILESTATUSRQ, $pself->GetMd4));
	}
	
	sub WritePartsRequest {
		my($pself, @list) = @_;
		$pself->info(" $pself >> Requesting parts <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		$pself->{super}->Network->WriteData($pself->{socket}, pack("C V C H32 V V V V V V", PROTO_VERSION, 41, CP_PARTSRQ, $pself->GetMd4,$list[0],$list[1],$list[2],$list[3],$list[4],$list[5]));
	}
	
	
	sub CreateTag {
		my($pself, %args) = @_;
		
		my @taglist = ();
		foreach my $tagname (keys(%args)) {
			my $tx = { type => 0x03, tagsize=>0x01, tagname=>undef, tagdata=>undef };
			if($tagname eq 'Nickname') {
				$tx->{tagname} = TAG_NICKNAME;
				$tx->{type}    = 0x02; # String
				my $nick = $args{$tagname};
				$tx->{tagdata} = pack("v",length($nick)).$nick;
			}
			elsif($tagname eq 'Version') {
				$tx->{tagname} = TAG_PROTOVERS;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'Flags') {
				$tx->{tagname} = TAG_FLAGS;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'EmuleVersion') {
				$tx->{tagname} = TAG_EMULEVERS;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'EmuleMisc1') {
				$tx->{tagname} = TAG_EMULEMISC1;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'EmuleMisc2') {
				$tx->{tagname} = TAG_EMULEMISC2;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'CompatibleClient') {
				$tx->{tagname} = TAG_COMPCLIENT;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'TcpPort') {
				$tx->{tagname} = TAG_TCPPORT;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			elsif($tagname eq 'UdpPort') {
				$tx->{tagname} = TAG_UDPPORT;
				$tx->{tagdata} = pack("V", $args{$tagname});
			}
			else {
				$pself->panic("Unknown Tagname '$tagname'");
			}
			push(@taglist, pack("C v C", $tx->{type}, $tx->{tagsize}, $tx->{tagname}).$tx->{tagdata});
		}
		return @taglist;
	}
	
	
	sub ParseMessage {
		my($pself,$payload) = @_;
		
		my $len        = length($payload);
		my $off        = -1;
		my $proto_vers = -1;
		my $size       = -1;
		my $opcode     = -1;
		warn ">> We got $len bytes\n";
		
		if($len >= 6) {
			($proto_vers, $size, $opcode) = unpack("C V C", $payload);
			warn " |--> Protocol      = $proto_vers\n";
			warn " |--> Size of chunk = $size\n";
			warn " |--> Opcode        = $opcode\n";
			
			if($len >= $size+5) {
				$off     = $size+5;
				$payload = substr($payload,6); # Ditch parsed header (ProtoVers, Size, Opcode)
				
				if($proto_vers == PROTO_HASZLIB) {
					# Decompress zlib message 'on-the-fly'
					$payload = Compress::Zlib::uncompress($payload);
				}
				
				if($opcode == CP_HELLO or $opcode == CP_HELLOREPLY) {
					my($hashsize)         = unpack("C",$payload);
					my $hash              = unpack("H*",substr($payload, 1, $hashsize));
					my($client_id, $port) = unpack("V v", substr($payload, 1+$hashsize));
					$payload              = { hash=>$hash, client_id=>$client_id, client_port=>$port };
				}
				elsif($opcode == OP_SERVERMSG) {
					my($slen) = unpack("v",$payload);
					$payload  = substr($payload, 2, $slen);
				}
				elsif($opcode == OP_IDCHANGE or $opcode== OP_SERVERINFO) {
					my($cid, $cbitmap) = unpack("V V",$payload);
					$payload = [$cid, $cbitmap];
				}
				elsif($opcode == OP_SEARCHRES) {
					my($results) = unpack("V", $payload);
					$payload = $pself->_ParseSearchResults($results,substr($payload,4));
				}
				elsif($opcode == OP_FOUNDSOURCES) {
					my($md4, $sources) = unpack("H32 C", $payload);
					$pself->info("Server sent me $sources sources for $md4");
					my $xoff = 17;
					for(1..$sources) {
						my($a,$b,$c,$d, $port) = unpack("CCCC v", substr($payload, $xoff));
						$xoff += 6;
						$pself->info("$md4 ---> $a.$b.$c.$d:$port");
						$pself->{_super}->_Command_Econnect($md4, "$a.$b.$c.$d", $port) if $d;
					}
				}
				elsif($opcode == CP_HASHSETREPLY) {
					my($md4, $numhashes) = unpack("H32 v",$payload);
					$payload = substr($payload,18);
					my @hashset = ();
					while(length($payload) > 0) {
						push(@hashset, unpack("H32",substr($payload,0,16)));
						$payload = substr($payload,16);
					}
					$payload = { hashset=>\@hashset, md4=>$md4 };
				}
				else {
					warn sprintf("Unparsed opcode 0x%X\n",$opcode);
					print unpack("H*", $payload)."\n\n";
				}
			}
		}
		return({payload=>$payload, opcode=>$opcode, offset=>$off});
	}
	
	
	sub _ParseSearchResults {
		my($pself, $rnum, $data) = @_;
		
		my $off = 0;
		my $val = 0;
		my $result = {};
		for(1..$rnum) {
			my($md4, $client, $port, $numtags) = unpack("H32 V v V", substr($data,$off));
			$off += 26;
			
			$result->{$md4} = { client=>$client, port=>$port, tags=>{} };
			
			for(1..$numtags) {
				my($type, $taglen) = unpack("C v", substr($data, $off));
				$off += 3;
				
				if($type >= 0x80) {
					# No taglen..
					$taglen  = 1;
					$off    -= 2;
					$type   -= 0x80;
				}
				
				my $tagname = substr($data, $off, $taglen);				
				$off += $taglen;
				
				if($type == 0x02) {
					my($slen) = unpack("v", substr($data,$off));
					$off += 2;
					$val = substr($data, $off, $slen);
					$off += $slen;
				}
				elsif($type == 0x03) {
					$val = unpack("V", substr($data, $off));
					$off += 4;
				}
				elsif($type == 0x09) {
					$val = unpack("C", substr($data, $off));
					$off++;
				}
				elsif($type == 0x14) {
					$val = substr($data, $off, 4);
					$off += 4;
				}
				else {
					printf("Unknown tagtype: %X\n%s",$type, unpack("H*",$data));
					goto PSR_FAIL;
				}
				$result->{$md4}->{tags}->{unpack("H*",$tagname)} = $val;
			}
		}
		PSR_FAIL:
		return $result;
	}
	
	
	sub HandleServerMessage {
		my($pself, $rx) = @_;
		warn "$pself ; $rx\n";
		if($rx->{opcode} == OP_SERVERMSG) {
			foreach my $this_msg (split(/\n/,$rx->{payload})) {
				$pself->{super}->Admin->SendNotify($this_msg);
			}
		}
		elsif($rx->{opcode} == OP_SERVERINFO) {
			$pself->info("Server <$pself> has $rx->{payload}->[0] users and a total of $rx->{payload}->[1] files");
			$pself->SetStats(users=>$rx->{payload}->[0], files=>$rx->{payload}->[1]);
		}
		elsif($rx->{opcode} == OP_IDCHANGE) {
			$pself->info("Server set my id to $rx->{payload}->[0]");
			$pself->{client_id} = $rx->{payload}->[0];
		}
		elsif($rx->{opcode} == OP_SEARCHRES) {
			$pself->info("Search result follows...");
			my $pl = $rx->{payload};
			foreach my $md4 (keys(%$pl)) {
				my $name = $pl->{$md4}->{tags}->{'01'};
				my $size = $pl->{$md4}->{tags}->{'02'};
				$pself->info("$md4 | $name  => $size");
			}
			
		}
		else {
			$pself->info("Dropped opcode $rx->{opcode}");
		}
	}
	
	sub HandleClientMessage {
		my($pself, $rx) = @_;
		
		if($pself->Status == Bitflu::DownloadDonkey::STATE_AWAIT_IHELLO or
		   $pself->Status == Bitflu::DownloadDonkey::STATE_AWAIT_RHELLO) {
			$pself->info("Handling HELLO packet ($rx->{opcode})");
			$pself->SetPort(     $rx->{payload}->{client_port}  );
			$pself->SetClientId( $rx->{payload}->{client_id}    );
			
			if($pself->Status == Bitflu::DownloadDonkey::STATE_AWAIT_IHELLO) {
				$pself->info("<$pself> sent me an hello, sending a response");
				$pself->WriteHello(Response=>1);
			}
			else {
				$pself->info("<$pself> replied, fine!");
			}
			$pself->Status(Bitflu::DownloadDonkey::STATE_IDLE);
			if($pself->HasMd4) {
					$pself->WriteFileRequest;             # Request file;
					$pself->WriteFileStatusRequest;
			}
		}
		elsif($pself->Status == Bitflu::DownloadDonkey::STATE_IDLE) {
			$pself->info(sprintf("IdleClient <$pself> sent me 0x%X",$rx->{opcode}));
			if($pself->HasMd4 && $rx->{opcode} == CP_FILEREPLY) {
				$pself->info("<$pself> sent me a file reply");
			}
			elsif($pself->HasMd4 && $rx->{opcode} == CP_FILESTATUSRPLY) {
				$pself->info("<$pself> sent me a filestatus reply, sending a slot request");
				$pself->WriteSlotRequest;
			}
			elsif($rx->{opcode} == 0x55) {
				$pself->Slot(SLOT_ME_GRANTED,1);
				$pself->warn("Yiek! Got an upload slot!");
				$pself->WritePartsRequest(0,101,201,101,201,301);
			}
		}
		else {
			$pself->info("Client <$pself> is in unknown state: ".$pself->Status);
		}
	}
	

sub debug { my($self, $msg) = @_; $self->{super}->debug("Donkey  : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Donkey  : ".$msg);  }
sub stop  { my($self, $msg) = @_; $self->{super}->stop("Donkey  : ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("Donkey  : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Donkey  : ".$msg); }


1;
