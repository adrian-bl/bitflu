package Bitflu::SourcesBitTorrentKademlia;
################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt


use strict;
use constant SHALEN                => 20;
use constant K_BUCKETSIZE          => 8;
use constant K_ALPHA               => 3;  # How many locks we are going to provide per sha1
use constant K_QUERY_TIMEOUT       => 15; # How long we are going to hold a lock
use constant K_ALIVEHUNT           => 18; # Ping 18 random nodes each 18 seconds
use constant K_MAX_FAILS           => 5;
use constant K_DEADEND             => 3;
use constant K_REANNOUNCE          => 28*60; # ReAnnounce each 28 minutes
use constant KSTATE_PEERSEARCH     => 1;
use constant KSTATE_SEARCH_DEADEND => 2;
use constant KSTATE_SEARCH_MYSELF  => 3;
use constant K_REAL_DEADEND        => 3;

use constant TORRENTCHECK_DELY     => 23;

################################################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, lastrun => 0, xping => { list => {}, trigger => 0 },
	             _addnode => { totalnodes => 0, badnodes => 0, goodnodes => 0 },
	             checktorrents_at => 0,
	             initboot_at => $mainclass->Network->GetTime+20, blame_at => $mainclass->Network->GetTime+300,
	           };
	bless($self,$class);
	$self->{my_sha1}       = $self->GetRandomSha1Hash("/dev/urandom")                               or $self->panic("Unable to seed my_sha1");
	$self->{my_token_1}    = Digest::SHA1::sha1($self->GetRandomSha1Hash("/dev/urandom"))           or $self->panic("Unable to seed my_token_1");
	$self->{tcp_port}      = $self->{super}->Configuration->GetValue('torrent_port')                or $self->panic("'torrent_port' not set in configuration");
	$self->{udpsock}       = $mainclass->Network->NewUdpListen(ID=>$self, Port=>$self->{tcp_port})  or $self->panic("Unable to listen on port: $@");
	return $self;
}

################################################################################################
# Init plugin
sub init {
	my($self) = @_;
	$self->{super}->AddRunner($self) or $self->panic("Unable to add runner");
	$self->StartHunting(_switchsha($self->{my_sha1}),KSTATE_SEARCH_MYSELF); # Add myself to find close peers
	$self->{super}->Admin->RegisterCommand('khunt',   $self, 'Command_AddKhunt', "Starts hunt of given SHA1 Hash");
	$self->{super}->Admin->RegisterCommand('kdebug',   $self, 'Command_Kdebug', "Debug Kademlia");

	my $hookit = undef;
	
	# Search DownloadBitTorrent hook:
	foreach my $cc (@{$self->{super}->{_Runners}}) {
		if($cc =~ /^Bitflu::DownloadBitTorrent=/) {
			$hookit = $cc;
		}
	}
	$self->{bittorrent} = $hookit or $self->panic("Unable to locate BitTorrent plugin");
	$self->info("BitTorrent-Kademlia plugin loaded. Using udp port $self->{tcp_port}");
	return 1;
}


sub Command_AddKhunt {
	my($self,@args) = @_;
	
	my @A = ();
	push(@A, [undef, "Foo"]);
	my $sha1 = pack("H40",$args[0]);
	$self->StartHunting($sha1, KSTATE_PEERSEARCH) if $args[0];
	return({CHAINSTOP=>1, MSG=>\@A});
}

sub Command_KillKhunt {
	my($self,@args) = @_;
	
	my @A = ();
	push(@A, [undef, "Killeredized"]);
	my $sha1 = pack("H40",$args[0]);
	$self->StopHunting($sha1) if $args[0];
	return({CHAINSTOP=>1, MSG=>\@A});
}

sub Command_Kdebug {
	my($self,@args) = @_;
	
	my @A = ();
	my $nn = 0;
	my $nv = 0;
	push(@A, [1, "--== Kademlia Debug ==--"]);
	
	push(@A, [1, "Hashes we are hunting right now"]);
	foreach my $key (keys(%{$self->{huntlist}})) {
		push(@A,[3, " --> ".unpack("H*",$key)]);
	}
	
	push(@A, [1, "Known Kademlia Nodes"]);
	foreach my $val (values(%{$self->{_addnode}->{hashes}})) {
		push(@A, [undef, "sha1=>".unpack("H*",$val->{sha1}).", good=>$val->{good}, lastseen=>$val->{lastseen}, fails=>$val->{rfail}, Ip=>".$val->{ip}.":".$val->{port}]);
		$nn++;
		$nv++ if $val->{good};
	}
	
	push(@A, [2, "We got $nn kademlia nodes. $nv are verified"]);
	
	
	return({CHAINSTOP=>1, MSG=>\@A});
}



################################################################################################
# Mainsub called by bitflu.pl
sub run {
	my($self) = @_;
	
	my $NOWTIME = $self->{super}->Network->GetTime;

	$self->{super}->Network->Run($self, {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});

	return if $self->{lastrun} == $NOWTIME;
	$self->{lastrun} = $NOWTIME;
	
	
	
#	print "We got: $self->{_addnode}->{totalnodes} ; $self->{_addnode}->{badnodes} ; -> good: $self->{_addnode}->{goodnodes} ; $self->{initboot_at}\n";
	
	if($self->{initboot_at} && $self->{initboot_at} < $NOWTIME) {
		$self->{initboot_at} = 0;
		
		if($self->{_addnode}->{goodnodes} == 0) {
			$self->{super}->Admin->SendNotify("Starting kademlia bootstrap. (Is udp:$self->{tcp_port} open?)");
			$self->BootFromPeer({ip=>'72.20.34.145', port=>6881});
			$self->BootFromPeer({ip=>'38.99.5.32', port=>6881});
		}
	}
	elsif($self->{blame_at} && $self->{blame_at} < $NOWTIME) {
		$self->{blame_at} = 0;
		if($self->{_addnode}->{totalnodes} < 80 or $self->{_addnode}->{goodnodes} < 20) {
			$self->{super}->Admin->SendNotify("Kademlia: Got $self->{_addnode}->{totalnodes} nodes ($self->{_addnode}->{goodnodes} are verified). Does your firewall block udp:$self->{tcp_port} ?");
		}
	}
	
	if($self->{checktorrents_at} < $NOWTIME) {
		$self->{checktorrents_at} = $NOWTIME + TORRENTCHECK_DELY;
		$self->CheckCurrentTorrents;
	}
	
	
	
	foreach my $huntkey (List::Util::shuffle keys(%{$self->{huntlist}})) {
		my $cached_best_bucket  = $self->{huntlist}->{$huntkey}->{bestbuck};
		my $cached_last_huntrun = $self->{huntlist}->{$huntkey}->{lasthunt};
		my $cstate              = $self->{huntlist}->{$huntkey}->{state};
		my $running_qtype       = undef;
		my $victims             = undef;
		next unless defined($cached_best_bucket);           # 0 nodes in table
		next if ($cached_last_huntrun > ($NOWTIME)-(K_QUERY_TIMEOUT)); # still searching
		$self->{huntlist}->{$huntkey}->{lasthunt} = $NOWTIME;
		
		if($cached_best_bucket == $self->{huntlist}->{$huntkey}->{deadend_lastbestbuck}) {
			$self->{huntlist}->{$huntkey}->{deadend}++;
		}
		else {
			$self->{huntlist}->{$huntkey}->{deadend_lastbestbuck} = $cached_best_bucket;
			$self->{huntlist}->{$huntkey}->{deadend} = 0;
		}
		
		if($self->{huntlist}->{$huntkey}->{deadend} >= K_REAL_DEADEND) {
			$self->{huntlist}->{$huntkey}->{deadend}  = 0;
			$self->{huntlist}->{$huntkey}->{lasthunt} = $NOWTIME + (K_QUERY_TIMEOUT*2); # Buy us some time
			if($cstate == KSTATE_PEERSEARCH) {
				# Wowies: ReSearch for a dead_end
				$self->SetState($huntkey,KSTATE_SEARCH_DEADEND);
				$self->TriggerHunt($huntkey);
			}
			elsif($cstate == KSTATE_SEARCH_DEADEND) {
				
				if($self->{huntlist}->{$huntkey}->{lastannounce} > ($NOWTIME)-(K_REANNOUNCE)) {
					my $peers = $self->ReAnnounceOurself($huntkey);
					$self->info(unpack("H*",$huntkey).": Announced at $peers nodes");
					$self->{huntlist}->{$huntkey}->{lastannounce} = $NOWTIME if $peers > 0;
				}
				
				$self->SetState($huntkey,KSTATE_PEERSEARCH);
			}
			next;
		}
		
		if($cstate == KSTATE_PEERSEARCH) {
			$running_qtype = "command_getpeers"; # We are searching for VALUES
		}
		elsif($cstate == KSTATE_SEARCH_DEADEND) {
			$running_qtype = "command_findnode"; # We search better peers
		}
		elsif($cstate == KSTATE_SEARCH_MYSELF) {
			$running_qtype = "command_findnode"; # We are searching near peers
		}
		else {
			$self->panic("Unhandled state for ".unpack("H*",$huntkey).": $cstate");
		}
		
		my $xlog = ">> ".unpack("H*",$huntkey)." ; STATE: $cstate ; BESTBUCK: $cached_best_bucket ; Q: $running_qtype ; DE $self->{huntlist}->{$huntkey}->{deadend}";

		for(my $i=$cached_best_bucket; $i >= 0; $i--) {
			next unless defined($self->{huntlist}->{$huntkey}->{buckets}->{$i}); # -> Bucket empty
			foreach my $buckref (@{$self->{huntlist}->{$huntkey}->{buckets}->{$i}}) { # Fixme: We should REALLY get the 3 best, not random
				my $lockstate = $self->GetAlphaLock($huntkey,$buckref);
				
				if($lockstate == 1) { # Just freshly locked
					$self->debug($xlog." ; $buckref->{ip}");
					$self->UdpWrite({ip=>$buckref->{ip}, port=>$buckref->{port}, cmd=>$self->$running_qtype($huntkey)});
				}
				if(++$victims >= K_ALPHA) {
					$i=-1;
					last;
				}
			}
		}
	}
	
	
	# Ping some nodes to check if them are still alive
	$self->AliveHunter();
	# Really remove killed nodes
	$self->RunKillerLoop();

	
	
}


sub _Network_Data {
	my($self,$sock,$buffref) =@_;
	
	my $THIS_IP   = $sock->peerhost();
	my $THIS_PORT = $sock->peerport();
	my $THIS_BUFF = $$buffref;
	
	if(length($THIS_BUFF) == 0) {
		$self->warn("$THIS_IP:$THIS_PORT sent no data");
		return;
	}
	
	my $btdec     = Bitflu::DownloadBitTorrent::Bencoding::decode($THIS_BUFF);
	
	if(ref($btdec) ne "HASH" or !defined($btdec->{t})) {
		$self->info("Garbage received from $THIS_IP:$THIS_PORT");
		return;
	}

		if($btdec->{y} eq 'q') {
			# -> QUERY
			
			# Check if query fulfills basic syntax
			if(length($btdec->{a}->{id}) != SHALEN or $btdec->{a}->{id} eq $self->{my_sha1}) {
				$self->info("$THIS_IP:$THIS_PORT ignoring malformed query");
				return;
			}
			
			unless(defined($self->AddNode({ip=>$THIS_IP, port=>$THIS_PORT, sha1=>$btdec->{a}->{id}}))) {
				$self->debug("$THIS_IP:$THIS_PORT has been rejected by AddNode");
				return;
			}
			
			
			# -> Requests sent to us
			if($btdec->{q} eq "ping") {
				$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_ping($btdec)});
				$self->info("$THIS_IP:$THIS_PORT : Pong reply sent");
			}
			elsif($btdec->{q} eq 'find_node' && length($btdec->{a}->{target}) == SHALEN) {
				my $aref = $self->GetNearestGoodFromSelfBuck($btdec->{a}->{target});
				my $nbuff = undef;
				foreach my $r (@$aref) { $nbuff .= _encodeNode($r); }
				$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_findnode($btdec,$nbuff)});
				$self->info("$THIS_IP:$THIS_PORT (find_node) : sent ".int(@$aref)." kademlia nodes to peer");
			}
			elsif($btdec->{q} eq 'get_peers' && length($btdec->{a}->{info_hash}) == SHALEN) {
				my $aref = $self->GetNearestGoodFromSelfBuck($btdec->{a}->{info_hash});
				my $nbuff = undef;
				foreach my $r (@$aref) { $nbuff .= _encodeNode($r); }
				$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_getpeers($btdec,$nbuff)});
				$self->info("$THIS_IP:$THIS_PORT (get_peers) : sent ".int(@$aref)." kademlia nodes to peer");
			}
			else {
				$self->info("Unhandled QueryType $btdec->{q}");
			}
		}
		elsif($btdec->{y} eq "r") {
			# -> Response
			my $peer_shaid = $btdec->{r}->{id};
			my $tr2hash    = $self->tr2hash($btdec->{t}); # Reply is for this SHA1
			
			if(length($peer_shaid) != SHALEN or $peer_shaid eq $self->{my_sha1}) {
				$self->info("$THIS_IP:$THIS_PORT ignoring malformed response");
				return;
			}
			
			if($self->{trustlist}->{"$THIS_IP:$THIS_PORT"}) {
				$self->info("$THIS_IP:$THIS_PORT -> Bootstrap done!");
				delete($self->{trustlist}->{"$THIS_IP:$THIS_PORT"});
				my $addnode = $self->AddNode({ip=>$THIS_IP,port=>$THIS_PORT, sha1=>$peer_shaid});
				if(!defined($addnode)) {
					$self->info("Bootnode not added!?");
					return;
				}
			}
			
			if(!defined($self->{_addnode}->{hashes}->{$peer_shaid})) {
				$self->info("$THIS_IP:$THIS_PORT (".unpack("H*",$peer_shaid).") sent response to unasked question. no thanks.");
				return;
			}
			elsif(length($tr2hash) != SHALEN) {
				$self->info("$THIS_IP:$THIS_PORT sent invalid hash TR");
				return;
			}
			
			
			$self->SetNodeAsGood({hash=>$peer_shaid,token=>$btdec->{r}->{token}});
			$self->FreeSpecificAlphaLock($tr2hash,$peer_shaid);
			
			if($btdec->{r}->{nodes}) {
				my $allnodes = _decodeNodes($btdec->{r}->{nodes});
				my $cbest    = $self->{huntlist}->{$tr2hash}->{bestbuck};
				my $numnodes = 0;
				foreach my $x (@$allnodes) {
					next if length($x->{sha1}) != SHALEN;
					next unless defined $self->AddNode($x);
					$numnodes++;
				}
				if( ($cbest < $self->{huntlist}->{$tr2hash}->{bestbuck}) && ($self->GetState($tr2hash) == KSTATE_PEERSEARCH ||
				     $self->GetState($tr2hash) == KSTATE_SEARCH_MYSELF)) {
					$self->TriggerHunt($tr2hash);
					$self->ReleaseAllAlphaLocks($tr2hash);
				}
			}
			elsif($btdec->{r}->{values}) {
				my $all_hosts = _decodeIPs($btdec->{r}->{values});
				$self->debug(unpack("H*",$tr2hash).": new BitTorrent nodes from $THIS_IP:$THIS_PORT");
				if($self->GetState($tr2hash) == KSTATE_PEERSEARCH) {
					$self->TriggerHunt($tr2hash);
					$self->SetState($tr2hash,KSTATE_SEARCH_DEADEND);
				}
				foreach my $bt_item (@$all_hosts) {
					$self->{bittorrent}->CreateNewOutgoingConnection(unpack("H*",$tr2hash),$bt_item->{ip},$bt_item->{port});
				}
			}
			else {
				$self->debug("$THIS_IP:$THIS_PORT: Ignoring packet without interesting content");
			}
		}
		else {
			$self->debug("$THIS_IP:$THIS_PORT: Ignored packet with suspect 'y' tag");
		}

	
}

sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self)."[Kademlia]: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self)." [Kademlia]: ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self)." [Kademlia]: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self)."[Kademlia]: ".$msg); }



sub CheckCurrentTorrents {
	my($self) = @_;
	my %known_torrents = map { $_ => 1 } $self->{bittorrent}->Torrent->GetTorrents;
	foreach my $hsha1 (keys(%{$self->{huntlist}})) {
		my $up_hsha1 = unpack("H40",$hsha1);
		next if $self->GetState($hsha1) == KSTATE_SEARCH_MYSELF;
		if($self->GetState($hsha1) == KSTATE_SEARCH_MYSELF) {
			next;
		}
		elsif(delete($known_torrents{$up_hsha1})) {
			# Torrent is present
		}
		else {
			$self->warn("Stopping hunt of $up_hsha1");
			$self->StopHunting($hsha1);
		}
	}
	
	foreach my $up_hsha1 (keys(%known_torrents)) {
		next if $self->{bittorrent}->Torrent->GetTorrent($up_hsha1)->IsPrivate;
		$self->StartHunting(pack("H40",$up_hsha1, KSTATE_PEERSEARCH));
	}
}






sub StartHunting {
	my($self,$sha,$initial_state) = @_;
	$self->panic("Invalid SHA1") if length($sha) != SHALEN;
	$self->panic("This SHA1 has been added") if defined($self->{huntlist}->{$sha});
	$self->debug("+ Hunt ".unpack("H*",$sha));
	my $trn = -1;
	for(64..150) {
		if(!defined $self->{trmap}->{chr($_)}) {
			$trn = $_;
			$self->{trmap}->{chr($trn)} = $sha;
			last;
		}
	}
	
	if($trn < 0) {
		$self->warn("No TR's left, too many huntjobs");
		return undef;
	}
	
	$self->{huntlist}->{$sha} = {addtime=>int(time()), trmap=>chr($trn), state=>($initial_state || KSTATE_PEERSEARCH)};
	
	foreach my $old_sha (keys(%{$self->{_addnode}->{hashes}})) {
		$self->_inject_node_into_huntbucket($old_sha,$sha);
	}
	return 1;
}


sub _inject_node_into_huntbucket {
	my($self,$new_node,$hunt_node) = @_;
	
	$self->panic("Won't inject non-existent node")          unless defined($self->{_addnode}->{hashes}->{$new_node});
	$self->panic("Won't inject into non-existent huntlist") unless defined($self->{huntlist}->{$hunt_node});
	
	my $bucket = int(_GetBucketIndexOf($new_node,$hunt_node));
	if(!defined($self->{huntlist}->{$hunt_node}->{buckets}->{$bucket}) or int(@{$self->{huntlist}->{$hunt_node}->{buckets}->{$bucket}}) < K_BUCKETSIZE) {
		push(@{$self->{huntlist}->{$hunt_node}->{buckets}->{$bucket}}, $self->{_addnode}->{hashes}->{$new_node});
		$self->{_addnode}->{hashes}->{$new_node}->{refcount}++;
		$self->{huntlist}->{$hunt_node}->{bestbuck} = $bucket if $bucket >= $self->{huntlist}->{$hunt_node}->{bestbuck}; # Set BestBuck cache
		$self->debug("+ Injected ; refcount is now $self->{_addnode}->{hashes}->{$new_node}->{refcount}");
		return 1;
	}
	return undef;
}


sub StopHunting {
	my($self,$sha) = @_;
	$self->panic("Unable to remove non-existent $sha") unless defined($self->{huntlist}->{$sha});
	$self->panic("Killing my own SHA1-ID is not permitted") if $self->{huntlist}->{$sha}->{state} == KSTATE_SEARCH_MYSELF;
	
	my $xtr = $self->{huntlist}->{$sha}->{trmap};
	$self->panic("No TR for $sha!") unless $xtr;
	foreach my $val (values %{$self->{huntlist}->{$sha}->{buckets}}) {
		foreach my $ref (@$val) {
			$ref->{refcount}--;
			
			if($ref->{refcount} != $self->{_addnode}->{hashes}->{$ref->{sha1}}->{refcount}) {
				$self->panic("NonRefRefcount: $ref->{refcount} ; $self->{_addnode}->{hashes}->{$ref->{sha1}}->{refcount}");
			}
			elsif($ref->{refcount} == 0) {
				$self->KillNode($ref->{sha1});
			}
			elsif($ref->{refcount} < 1) {
				$self->panic("Assert refcount >= 1 failed; $ref->{refcount}");
			}
		}
	}
	delete($self->{trmap}->{$xtr});
	delete($self->{huntlist}->{$sha});
	return 1;
}


sub SetState {
	my($self,$sha,$state) = @_;
	$self->panic("Invalid SHA: $sha") unless defined($self->{huntlist}->{$sha});
	return $self->{huntlist}->{$sha}->{state} = $state;
}

sub GetState {
	my($self,$sha) = @_;
	$self->panic("Invalid SHA: $sha") unless defined($self->{huntlist}->{$sha});
	return $self->{huntlist}->{$sha}->{state};
}

sub TriggerHunt {
	my($self,$sha) = @_;
	$self->panic("Invalid SHA: $sha") unless defined($self->{huntlist}->{$sha});
	$self->debug(unpack("H*",$sha)." -> hunt trigger");
	return $self->{huntlist}->{$sha}->{lasthunt} = 0;
}


########################################################################
# Switch last 2 sha1 things
sub _switchsha {
	my($string) = @_;
	$string = unpack("H*",$string);
	my $new = substr($string,0,-2).substr($string,-1,1).substr($string,-2,1);
	return pack("H*",$new);
}


sub tr2hash {
	my($self,$chr) = @_;
	return ($self->{trmap}->{$chr});
}


########################################################################
# Computes an *expensive* and maybe *good* SHA1 hash
sub GetRandomSha1Hash {
	my($self,$rnd) = @_;
	open(RAND, $rnd) or return $self->panic("Unable to open $rnd : $!");
	my $buff = undef;
	sysread(RAND,$buff,160*3);
	close(RAND);
	return Digest::SHA1::sha1($buff);
}

sub BootFromPeer {
	my($self,$ref) = @_;
	
	$self->{trustlist}->{$ref->{ip}.":".$ref->{port}}++;
	
	$ref->{cmd} = $self->command_findnode(_switchsha($self->{my_sha1}));
	$self->UdpWrite($ref);
	$self->info("Booting using $ref->{ip}:$ref->{port}");
}


sub UdpWrite {
	my($self,$r) = @_;
	my $btcmd = Bitflu::DownloadBitTorrent::Bencoding::encode($r->{cmd});
	$self->{super}->Network->SendUdp($self->{udpsock}, Ip=>$r->{ip}, Port=>$r->{port}, Data=>$btcmd);
}



########################################################################
# Mark node as killable
sub KillNode {
	my($self,$sha1) = @_;
	$self->panic("Invalid SHA: $sha1") unless defined($self->{_addnode}->{hashes}->{$sha1});
	$self->{_killnode}->{$sha1}++;
}

########################################################################
# Add a node to our internal memory-only blacklist
sub BlacklistBadNode {
	my($self,$ref) = @_;
	my $k = $ref->{ip}.":".$ref->{port};
	$self->{_knownbad}->{$k} ||= int(time());
	return undef;
}

########################################################################
# Check if a node is blacklisted
sub NodeIsBlacklisted {
	my($self,$ref) = @_;
	my $k = $ref->{ip}.":".$ref->{port};
	return defined($self->{_knownbad}->{$k}); # Fixme: We shall expire them
}


########################################################################
# Return commands to announce ourself
sub ReAnnounceOurself {
	my($self,$sha) = @_;
	$self->panic("Invalid SHA: $sha") unless defined($self->{huntlist}->{$sha});
	my $NEAR = $self->GetNearestNodes($sha,K_BUCKETSIZE,1);
	my @UDPCMD = ();
	my $count = 0;
	foreach my $r (@$NEAR) {
		next if(length($r->{token}) != SHALEN);
		my $cmd = {ip=>$r->{ip}, port=>$r->{port}, cmd=>$self->command_announce($sha,$r->{token})};
		$self->UdpWrite($cmd);
		$count++;
	}
	return $count;
}


########################################################################
# Returns the $nodenum nearest nodes ; FIXME: We do no walk-up and no XOR
sub GetNearestNodes {
	my($self,$sha,$nodenum,$onlygood) = @_;
	$self->panic("Invalid SHA: $sha") unless defined($self->{huntlist}->{$sha});
	$nodenum ||= K_BUCKETSIZE;
	my @BREF = ();
	for(my $i=$self->{huntlist}->{$sha}->{bestbuck}; $i >= 0; $i--) {
		next unless defined($self->{huntlist}->{$sha}->{buckets}->{$i}); # Empty bucket
		foreach my $buckref (@{$self->{huntlist}->{$sha}->{buckets}->{$i}}) { # Fixme: We shall XorSort them!
			next if $onlygood && $buckref->{good} == 0;
			push(@BREF,$buckref);
			if(--$nodenum < 1) { $i = -1 ; last; }
		}
	}
	return \@BREF;
}

sub GetNearestGoodFromSelfBuck {
	my($self,$target) = @_;
	
	my $nodenum = K_BUCKETSIZE;
	my $sha = _switchsha($self->{my_sha1});
	my @TMP = ();
	my @R   = ();
	for(my $i=$self->{huntlist}->{$sha}->{bestbuck}; $i >= 0; $i--) {
		next unless defined($self->{huntlist}->{$sha}->{buckets}->{$i});
		foreach my $buckref (@{$self->{huntlist}->{$sha}->{buckets}->{$i}}) {
			next if $buckref->{good} == 0;
			my $bucket_index = _GetBucketIndexOf($buckref->{sha1},$target);
			push(@{$TMP[$bucket_index]},$buckref);
		}
	}
	foreach my $a (reverse(@TMP)) {
		next unless ref($a) eq "ARRAY";
		foreach my $r (@$a) {
			push(@R,$r);
			return \@R if --$nodenum < 1;
		}
	}
	
	return \@R;
}

sub AliveHunter {
	my($self) = @_;
	my $NOWTIME = int(time());
	if($self->{xping}->{trigger} < $NOWTIME-(K_ALIVEHUNT)) {
		$self->{xping}->{trigger} = $NOWTIME;
		my $credits = K_ALIVEHUNT - int(keys(%{$self->{xping}->{list}}));
		
		foreach my $r (List::Util::shuffle values(%{$self->{_addnode}->{hashes}})) {
			if(!defined($self->{xping}->{list}->{$r->{sha1}}) && ($r->{good} == 0 or ($r->{good} != 0 && $r->{lastseen}+300 > $NOWTIME))) {
				$credits--;
				$self->{xping}->{list}->{$r->{sha1}} = 0; # No reference; copy it!
			}
			last if $credits < 1;
		}
		
		foreach my $sha1 (keys(%{$self->{xping}->{list}})) {
			if(!defined($self->{_addnode}->{hashes}->{$sha1})) { delete $self->{xping}->{list}->{$sha1}; } # Vanished
			else {
				if($self->{xping}->{list}->{$sha1} == 0) {
					$self->{xping}->{list}->{$sha1}++;
				}
				else {
					$self->{_addnode}->{hashes}->{$sha1}->{rfail}++;
				}
				
				if($self->{_addnode}->{hashes}->{$sha1}->{rfail} >= K_MAX_FAILS) {
					$self->KillNode($sha1);
					$self->BlacklistBadNode($self->{_addnode}->{hashes}->{$sha1});
				}
				else {
					my $cmd = $self->command_ping(_switchsha($self->{my_sha1}));
					$self->UdpWrite({ip=>$self->{_addnode}->{hashes}->{$sha1}->{ip}, port=>$self->{_addnode}->{hashes}->{$sha1}->{port},cmd=>$cmd});
				}
				
			}
		}
	}
}

########################################################################
# Requests a LOCK for given $hash using ip-stuff $ref
# Returns '1' if you got a lock
# Returns '-1' if the node was locked
# Returns undef if you are out of locks
sub GetAlphaLock {
	my($self,$hash,$ref) = @_;
	
	$self->panic("Invalid hash")      if !$self->{huntlist}->{$hash};
	$self->panic("Invalid node hash") if length($ref->{sha1}) != SHALEN;
	my $NOWTIME = int(time());
	my $islocked = 0;
	my $isfree   = 0;
	
	for my $lockn (1..K_ALPHA) {
		if(!exists($self->{huntlist}->{$hash}->{"lockn_".$lockn})) {
			$isfree = $lockn;
		}
		elsif($self->{huntlist}->{$hash}->{"lockn_".$lockn}->{locktime} <= ($NOWTIME-(K_QUERY_TIMEOUT))) {
			# Remove thisone:
			my $topenalty = $self->{huntlist}->{$hash}->{"lockn_".$lockn}->{sha1} or $self->panic("Lock #$lockn had no sha1");
			delete($self->{huntlist}->{$hash}->{"lockn_".$lockn})                 or $self->panic("Failed to remove lock!");
			$isfree = $lockn;
			if($topenalty && defined($self->{_addnode}->{hashes}->{$topenalty})) {
				if(++$self->{_addnode}->{hashes}->{$topenalty}->{rfail} >= K_MAX_FAILS) {
					$self->KillNode($topenalty);
					$self->BlacklistBadNode($self->{_addnode}->{hashes}->{$topenalty});
				}
			}
		}
		elsif($self->{huntlist}->{$hash}->{"lockn_".$lockn}->{sha1} eq $ref->{sha1}) {
			$islocked = 1;
		}
	}
	
	if($islocked) { return -1; }
	elsif($isfree) {
		$self->{huntlist}->{$hash}->{"lockn_".$isfree}->{sha1} = $ref->{sha1} or $self->panic("Ref has no SHA1");
		$self->{huntlist}->{$hash}->{"lockn_".$isfree}->{locktime} = $NOWTIME;
		return 1;
	}
	else {
		return undef; # No locks free
	}
	
}

########################################################################
# Try to free a lock for given sha1 node
sub FreeSpecificAlphaLock {
	my($self,$lockhash,$peersha) = @_;
	$self->panic("Invalid hash")      if !$self->{huntlist}->{$lockhash};
	$self->panic("Invalid node hash") if length($peersha) != SHALEN;
	for my $lockn (1..K_ALPHA) {
		if(exists($self->{huntlist}->{$lockhash}->{"lockn_".$lockn}) && $self->{huntlist}->{$lockhash}->{"lockn_".$lockn}->{sha1} eq $peersha) {
			$self->debug("Releasing lock $lockn");
			delete($self->{huntlist}->{$lockhash}->{"lockn_".$lockn});
			return;
		}
	}
}

########################################################################
# Free all locks for given hash
sub ReleaseAllAlphaLocks {
	my($self,$hash) = @_;
	$self->panic("Invalid hash") if !$self->{huntlist}->{$hash};
	for my $lockn (1..K_ALPHA) {
		delete($self->{huntlist}->{$hash}->{"lockn_".$lockn});
	}
	return undef;
}


########################################################################
# Decode Nodes
sub _decodeNodes {
	my($buff) = @_;
	my @ref = ();
	my $bufflen = length($buff);
	for(my $i=0; $i<$bufflen; $i+=26) {
		my $chunk = substr($buff,$i,26);
		my $nodeID =  substr($chunk,0,20);
		my $a    = unpack("C", substr($chunk,20,1));
		my $b    = unpack("C", substr($chunk,21,1));
		my $c    = unpack("C", substr($chunk,22,1));
		my $d    = unpack("C", substr($chunk,23,1));
		my $port = unpack("n", substr($chunk,24,2));
		my $IP = "$a.$b.$c.$d";
		push(@ref, {ip=>$IP, port=>$port, sha1=>$nodeID});
	}
	return \@ref;
}

########################################################################
# Creates a single NODES encoded entry
sub _encodeNode {
	my($r) = @_;
	my $buff   = $r->{sha1};
	my $ip     = $r->{ip};
	my $port   = $r->{port};

	my $funny_assert = 0;
	foreach my $cx (split(/\./,$ip)) { $buff .= pack("C",$cx); $funny_assert++; }
	Carp::confess("BUGBUG => $ip") if $funny_assert != 4;
	$buff .= pack("n",$port);
	return $buff;
}

########################################################################
# Decode IPs
sub _decodeIPs {
	my($ax) = @_;
	my @ref = ();
	foreach my $chunk (@$ax) {
		my $a = unpack("C",substr($chunk,0,1));
		my $b = unpack("C",substr($chunk,1,1));
		my $c = unpack("C",substr($chunk,2,1));
		my $d = unpack("C",substr($chunk,3,1));
		my $p = unpack("n",substr($chunk,4,2));
		push(@ref, {ip=>"$a.$b.$c.$d", port=>$p});
	}
	return \@ref;
}

########################################################################
# Pong node
sub reply_ping {
	my($self,$bt) = @_;
	return { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}} };
}

########################################################################
# Send find_node result to peer
sub reply_findnode {
	my($self,$bt,$payload) = @_;
	return { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, nodes=>$payload} };
}

########################################################################
# Send find_node result to peer
sub reply_getpeers {
	my($self,$bt,$payload) = @_;
	return { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, token=>$self->{my_token_1}, nodes=>$payload} };
}


########################################################################
# Assemble GetPeers request
sub command_getpeers {
	my($self,$ih) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	return { t=>$tr, y=>'q', q=>'get_peers', a=>{id=>$self->{my_sha1}, info_hash=>$ih}};
}

########################################################################
# Assemble a ping request
sub command_ping {
	my($self,$ih) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	return { t=>$tr, y=>'q', q=>'ping',      a=>{id=>$self->{my_sha1}}};
}

########################################################################
# Assemble an announce request
sub command_announce {
	my($self,$ih,$key) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	$self->panic("Invalid key: $key") if length($key) != SHALEN;
	return { t=>$tr, y=>'q', q=>'announce_peer', a=>{id=>$self->{my_sha1}, port=>$self->{tcp_port}, info_hash=>$ih, token=>$key}};
}

########################################################################
# Assemble FindNode request
sub command_findnode {
	my($self,$ih) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	return { t=>$tr, y=>'q', q=>'find_node', a=>{id=>$self->{my_sha1}, target=>$ih}};
}


########################################################################
# Set status of a KNOWN node to 'good'
sub SetNodeAsGood {
	my($self, $ref) = @_;
	
	my $xid = $ref->{hash};
	if(defined($self->{_addnode}->{hashes}->{$xid})) {
		if($self->{_addnode}->{hashes}->{$xid}->{good} == 0) {
			$self->{_addnode}->{badnodes}--;
			$self->{_addnode}->{goodnodes}++;
			$self->{_addnode}->{hashes}->{$xid}->{good} = 1;
			$self->{_addnode}->{hashes}->{$xid}->{rfail} = 0;
		}
		if(length($ref->{token}) == SHALEN) {
			$self->{_addnode}->{hashes}->{$xid}->{token} = $ref->{token};
		}
		$self->{_addnode}->{hashes}->{$xid}->{lastseen} = int(time());
	}
	else {
		$self->panic("Unable to set $xid as good because it does NOT exist!");
	}
	delete($self->{xping}->{list}->{$xid}); # No need to ping it again
}

########################################################################
# Add note to routing table
sub AddNode {
	my($self, $ref) = @_;
	my $xid = $ref->{sha1};
	$self->panic("Invalid SHA1: $xid") if length($xid) != SHALEN;
	
	my $NOWTIME = int(time());
	
	if($xid eq $self->{my_sha1}) {
		$self->debug("AddNode($self,$ref): Not adding myself!");
		return undef;
	}
	elsif($self->NodeIsBlacklisted($ref)) {
		$self->debug("AddNode($self,$ref): Node is blacklisted, not added");
		return undef;
	}
	
	if(!defined($self->{_addnode}->{hashes}->{$xid})) {
		# This is a new SHA ID
		$self->{_addnode}->{hashes}->{$xid} = { addtime=>$NOWTIME, lastseen=>$NOWTIME, token=>'', rfail=>0, good=>0, sha1=>$xid , ip=>$ref->{ip}, port=>$ref->{port} };
		
		# Insert references to all huntlist items
		foreach my $k (keys(%{$self->{huntlist}})) {
			$self->_inject_node_into_huntbucket($xid,$k);
		}
		
		if($self->{_addnode}->{hashes}->{$xid}->{refcount} == 0) {
			$self->debug("Insertation rollback: no free buck for thisone!");
			delete($self->{_addnode}->{hashes}->{$xid});
			return undef;
		}
		else {
			$self->{_addnode}->{totalnodes}++;
			$self->{_addnode}->{badnodes}++;
			return 1;
		}
	}
	else {
		# We know this node, only update lastseen
		$self->{_addnode}->{hashes}->{$xid}->{lastseen} = $NOWTIME;
		return 0;
	}
}




########################################################################
# Kill nodes added to _killnode
sub RunKillerLoop {
	my($self) = @_;
	foreach my $xkill (keys(%{$self->{_killnode}})) {
		$self->panic("Cannot kill non-existent node") unless $self->{_addnode}->{hashes}->{$xkill};
		
		my $nk = $self->{_addnode}->{hashes}->{$xkill};
		my $refcount = $nk->{refcount};
		foreach my $k (keys(%{$self->{huntlist}})) {
			my $ba = int(_GetBucketIndexOf($k,$xkill));
			if(ref($self->{huntlist}->{$k}->{buckets}->{$ba}) eq "ARRAY") {
				my $i = 0;
				foreach my $noderef (@{$self->{huntlist}->{$k}->{buckets}->{$ba}}) {
					if($noderef->{sha1} eq $xkill) {
						splice(@{$self->{huntlist}->{$k}->{buckets}->{$ba}},$i,1);
						$refcount--;
						last;
					}
					$i++;
				}
			}
		}
		$self->panic("Invalid refcount: $refcount") if $refcount != 0;
		# Fixme: Wir sollten BestBuck ev. anpassen!
		$self->{_addnode}->{totalnodes}--;
		if($nk->{good}) { $self->{_addnode}->{goodnodes}--; }
		else            { $self->{_addnode}->{badnodes}--; }
		delete($self->{_addnode}->{hashes}->{$xkill});
		delete($self->{xping}->{list}->{$xkill});
	}
	$self->{_killnode} = ();
}




########################################################################
# Returns BucketValue of 2 hashes
sub _GetBucketIndexOf {
	my($sha_1, $sha_2) = @_;
	my $b1 = unpack("B*", $sha_1);
	my $b2 = unpack("B*", $sha_2);
	my $bucklen   = length($b1);
	my $bucklen_2 = length($b2);
	Carp::confess("\$bucklen != \$bucklen_2 : $bucklen != $bucklen_2") if $bucklen != $bucklen_2;
	my $i = 0;
	for($i = 0; $i<$bucklen; $i++) {
		last if substr($b1,$i,1) ne substr($b2,$i,1);
	}
	return $i;
}


1;
