package Bitflu::SourcesBitTorrentKademlia;
################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2009 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt

#
# This is not the best Kademlia-Implementation in town.. but it works :-)
#

use strict;
use constant _BITFLU_APIVERSION    => 20091108;
use constant SHALEN                => 20;
use constant K_BUCKETSIZE          => 8;
use constant K_ALPHA               => 3;    # How many locks we are going to provide per sha1
use constant K_QUERY_TIMEOUT       => 15;   # How long we are going to hold a lock
use constant K_ALIVEHUNT           => 18;   # Ping 18 random nodes each 18 seconds
use constant K_MAX_FAILS           => 5;
use constant K_REANNOUNCE          => 1800; # ReAnnounce each 30 minutes
use constant KSTATE_PEERSEARCH     => 1;
use constant KSTATE_SEARCH_DEADEND => 2;
use constant KSTATE_SEARCH_MYSELF  => 3;
use constant KSTATE_PAUSED         => 4;
use constant K_REAL_DEADEND        => 3;    # How many retries to do

use constant BOOT_TRIGGER_COUNT    => 20;      # Boot after passing 20 jiffies
use constant BOOT_SAVELIMIT        => 100;     # Do not save more than 100 kademlia nodes
use constant BOOT_KICKLIMIT        => 8;       # Query 8 nodes per boostrap

use constant TORRENTCHECK_DELY     => 23;    # How often to check for new torrents
use constant G_COLLECTOR           => 300;   # 'GarbageCollectr' + Rotate SHA1 Token after 5 minutes
use constant MAX_TRACKED_ANNOUNCE  => 250;   # How many torrents we are going to track
use constant MAX_TRACKED_PEERS     => 100;   # How many peers (per torrent) we are going to track
use constant MAX_TRACKED_SEND      => 30;    # Do not send more than 30 peers per request

use constant MIN_KNODES            => 5;     # Try to bootstrap until we reach this limit

use Data::Dumper;
$Data::Dumper::Useqq = 1;

################################################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	
	my $prototype = { super=>undef,lastrun => 0, xping => { list => {}, cache=>[], trigger => 0 },
	                 _addnode => { totalnodes => 0, badnodes => 0, goodnodes => 0 }, _killnode => {},
	                 huntlist => {}, checktorrents_at  => 0, gc_lastrun => 0, topclass=>undef,
	                 bootstrap_trigger => 0, bootstrap_credits => 0, announce => {},
	                };
	
	my $topself   = {super=>$mainclass, proto=>{} };
	bless($topself,$class);
	
	my @protolist = ();
	push(@protolist,4) if $mainclass->Network->HaveIPv4;
	push(@protolist,6) if $mainclass->Network->HaveIPv6;
	
	foreach my $proto (@protolist) {
		my $this = $mainclass->Tools->DeepCopy($prototype);
		bless($this,$class."::IPv$proto");
		$this->{super}         = $mainclass;
		$this->{topclass}      = $topself;
		$this->{my_sha1}       = $this->GetRandomSha1Hash("/dev/urandom")                      or $this->panic("Unable to seed my_sha1");
		$this->{my_token_1}    = $this->GetRandomSha1Hash("/dev/urandom")                      or $this->panic("Unable to seed my_token_1");
		$this->{protoname}     = "IPv$proto";
		$topself->{tcp_bind}   = $this->{tcp_bind} = ($mainclass->Configuration->GetValue('torrent_bind') || 0); # May be null
		$topself->{tcp_port}   = $this->{tcp_port} = $mainclass->Configuration->GetValue('torrent_port')           or $this->panic("'torrent_port' not set in configuration");
		$topself->{my_sha1}    = $this->{my_sha1}  = $mainclass->Tools->sha1(($mainclass->Configuration->GetValue('kademlia_idseed') || $this->{my_sha1}));
		$topself->{proto}->{$proto} = $this;
	}
	
	$mainclass->Configuration->SetValue('kademlia_idseed', 0) unless defined($mainclass->Configuration->GetValue('kademlia_idseed'));
	$mainclass->Configuration->RuntimeLockValue('kademlia_idseed');
	
	
	return $topself;
}

################################################################################################
# Init plugin
sub init {
	my($topself) = @_;
	
	
	my $udp_socket = $topself->{super}->Network->NewUdpListen(ID=>$topself, Bind=>$topself->{tcp_bind}, Port=>$topself->{tcp_port},
	                                  Callbacks => { Data => '_Network_Data' } ) or $topself->panic("Cannot create udp socket on $topself->{tcp_bind}:$topself->{tcp_port}");
	
	my $bt_hook    = $topself->{super}->GetRunnerTarget('Bitflu::DownloadBitTorrent');
	
	foreach my $proto (keys(%{$topself->{proto}})) {
		$topself->info("Firing up protocol $proto ...");
		
		my $this_self = $topself->{proto}->{$proto};
		
		$this_self->{bittorrent}        = $bt_hook or $topself->panic("Cannot add bittorrent hook");
		$this_self->StartHunting(_switchsha($this_self->{my_sha1}),KSTATE_SEARCH_MYSELF); # Add myself to find close peers
		$this_self->{super}->Admin->RegisterCommand('kdebug'.$proto    ,$this_self, 'Command_Kdebug'   , "ADVANCED: Dump Kademlia nodes");
		$this_self->{super}->Admin->RegisterCommand('kannounce'.$proto ,$this_self, 'Command_Kannounce', "ADVANCED: Dump tracked kademlia announces");
		$this_self->{bootstrap_trigger} = 1;
		$this_self->{bootstrap_credits} = 4; # Try to boot 4 times
		$this_self->{udpsock}           = $udp_socket;
	}
	
	
	$topself->{super}->AddRunner($topself) or $topself->panic("Cannot add runner");
	
	$topself->info("BitTorrent-Kademlia plugin loaded. Using udp port $topself->{tcp_port}, NodeID: ".unpack("H*",$topself->{my_sha1}));
	
	
	return 1;
}


################################################################################################
# Mainsub called by bitflu.pl
sub run {
	my($topself,$NOWTIME) = @_;
	
	foreach my $this_self (values(%{$topself->{proto}})) {
		$this_self->_proto_run($NOWTIME);
	}
	return 3;
}

################################################################################################
# Dispatch payload to correct network subclass
sub _Network_Data {
	my($topself,$sock,$buffref) = @_;
	
	my $THIS_IP = $sock->peerhost();
	
	if(exists($topself->{proto}->{6}) && $topself->{super}->Network->IsNativeIPv6($THIS_IP)) {
		$topself->{proto}->{6}->NetworkHandler($sock,$buffref,$THIS_IP);
	}
	elsif(exists($topself->{proto}->{4}) && $topself->{super}->Network->IsValidIPv4($THIS_IP) or
	      ($THIS_IP = $topself->{super}->Network->SixToFour($THIS_IP)) ) {
		$topself->{proto}->{4}->NetworkHandler($sock,$buffref,$THIS_IP);
	}
	else {
		$topself->warn("What is $THIS_IP ?!");
	}
}


################################################################################################
# Display all tracked torrents
sub Command_Kannounce {
	my($self,@args) = @_;
	
	my @A = ();
	push(@A, [undef, "Tracked torrents -> Own id: ".unpack("H*",$self->{my_sha1})]);
	foreach my $sha1 (keys(%{$self->{announce}})) {
		push(@A, [1, "=> ".unpack("H*",$sha1)]);
		foreach my $nid (keys(%{$self->{announce}->{$sha1}})) {
			my $ref = $self->{announce}->{$sha1}->{$nid};
			push(@A, [undef, "   ip => $ref->{ip} ; port => $ref->{port} ; seen => $ref->{seen}"]);
		}
	}
	
	return({OK=>1, MSG=>\@A, SCRAP=>[]});
}

################################################################################################
# Display debug information / nodes breakdown
sub Command_Kdebug {
	my($self,@args) = @_;
	
	my @A    = ();
	my $arg1 = ($args[0] || '');
	my $arg2 = ($args[1] || '');
	my $nn   = 0;
	my $nv   = 0;
	
	push(@A, [1, "--== Kademlia Debug ==--"]);
	
	
	push(@A, [1, "Known Kademlia Nodes"]);
	foreach my $val (values(%{$self->{_addnode}->{hashes}})) {
		push(@A, [undef, "sha1=>".unpack("H*",$val->{sha1}).", good=>$val->{good}, lastseen=>$val->{lastseen}, fails=>$val->{rfail}, Ip=>".$val->{ip}.":".$val->{port}]);
		$nn++;
		$nv++ if $val->{good};
	}
	
	push(@A, [4, "Hashes we are hunting right now"]);
	foreach my $key (keys(%{$self->{huntlist}})) {
		push(@A,[3, " --> ".unpack("H*",$key)]);
		push(@A,[1, "     BestBucket: ".$self->{huntlist}->{$key}->{bestbuck}." ; Announces: ".$self->{huntlist}->{$key}->{announce_count}."; State: ".$self->GetState($key)]);
	}
	
	
	if($arg1 eq '-v') {
		my $sha1 = pack("H*", $arg2);
		   $sha1 = _switchsha($self->{my_sha1}) unless exists $self->{huntlist}->{$sha1};
		my $bref = $self->{huntlist}->{_switchsha($self->{my_sha1})}->{buckets};
		
		push(@A, [0, '']);
		push(@A, [1, "Buckets of ".unpack("H*",$sha1)]);
		
		
		foreach my $bnum (sort({$a<=>$b} keys(%$bref))) {
			my $bs = int(@{$bref->{$bnum}});
			next unless $bs;
			push(@A, [2, sprintf("bucket # %3d -> $bs node(s)",$bnum)]);
		}
		
	}
	
	
	my $percent = sprintf("%5.1f%%", ($nn ? 100*$nv/$nn : 0));
	
	push(@A, [0, '']);
	push(@A, [0, "Current mode                   : ".($self->MustBootstrap ? 'bootstrap' : 'running')]);
	push(@A, [0, sprintf("Number of known kademlia peers : %5d",$nn) ]);
	push(@A, [0, sprintf("Good (reachable) nodes         : %5d (%s)",$nv,$percent) ]);
	push(@A, [0, sprintf("Ping-Cache size                : %5d", int(@{$self->{xping}->{cache}}))]);
	push(@A, [0, sprintf("Outstanding ping replies       : %5d", int(keys(%{$self->{xping}->{list}})))]);
	push(@A, [0, '']);
	return({MSG=>\@A, SCRAP=>[]});
}




################################################################################################
# Run !
sub _proto_run {
	my($self,$NOWTIME) = @_;
	
	if($self->{gc_lastrun} < $NOWTIME-(G_COLLECTOR)) {
		# Rotate SHA1 Token
		$self->{gc_lastrun} = $NOWTIME;
		$self->RotateToken;
		$self->AnnounceCleaner;
	}
	
	if($self->{bootstrap_trigger} && $self->{bootstrap_trigger}++ == BOOT_TRIGGER_COUNT) {
		$self->{bootstrap_trigger} = ( --$self->{bootstrap_credits} > 0 ? 1 : 0 ); # ReEnable only if we have credits left
		
		if($self->MustBootstrap) {
			$self->{super}->Admin->SendNotify("No kademlia $self->{protoname} peers, starting bootstrap... (Is udp:$self->{tcp_port} open?)");
			foreach my $node ($self->GetBootNodes) {
				$node->{ip} = $self->Resolve($node->{ip});
				next unless $node->{ip};
				$self->BootFromPeer($node);
			}
		}
		else {
			$self->{bootstrap_trigger} = 0; # We got some nodes -> Disable bootstrapping
		}
	}
	
	
	
	if($self->{checktorrents_at} < $NOWTIME) {
		$self->{checktorrents_at} = $NOWTIME + TORRENTCHECK_DELY;
		$self->CheckCurrentTorrents;
		$self->SaveBootNodes;
	}
	
	
	# Check each torrent/key (target) that we are hunting right now
	foreach my $huntkey (List::Util::shuffle keys(%{$self->{huntlist}})) {
		my $cached_best_bucket  = $self->{huntlist}->{$huntkey}->{bestbuck};
		my $cached_last_huntrun = $self->{huntlist}->{$huntkey}->{lasthunt};
		my $cstate              = $self->{huntlist}->{$huntkey}->{state};
		my $running_qtype       = undef;
		
		next if ($cached_last_huntrun > ($NOWTIME)-(K_QUERY_TIMEOUT)); # still searching
		next if $cstate == KSTATE_PAUSED;                              # Search is paused
		
		$self->{huntlist}->{$huntkey}->{lasthunt} = $NOWTIME;
		
		if($cached_best_bucket == $self->{huntlist}->{$huntkey}->{deadend_lastbestbuck}) {
			$self->{huntlist}->{$huntkey}->{deadend}++; # No progress made
		}
		else {
			$self->{huntlist}->{$huntkey}->{deadend_lastbestbuck} = $cached_best_bucket;
			$self->{huntlist}->{$huntkey}->{deadend} = 0;
		}
		
		
		if($self->{huntlist}->{$huntkey}->{deadend} >= K_REAL_DEADEND) { # Looks like we won't go anywhere..
			$self->{huntlist}->{$huntkey}->{deadend}  = 0;
			$self->{huntlist}->{$huntkey}->{lasthunt} = $NOWTIME + (K_QUERY_TIMEOUT*2); # Buy us some time
			if($cstate == KSTATE_PEERSEARCH) {
				# Switch mode -> search (again) for a deadend
				$self->SetState($huntkey,KSTATE_SEARCH_DEADEND);
				$self->TriggerHunt($huntkey);
			}
			elsif($cstate == KSTATE_SEARCH_DEADEND) {
				# We reached a deadend -> Announce (if we have to) and restart search.
				if($self->{huntlist}->{$huntkey}->{lastannounce} < ($NOWTIME)-(K_REANNOUNCE)) {
					my $peers = $self->ReAnnounceOurself($huntkey);
					
					if($peers > 0) {
						$self->{huntlist}->{$huntkey}->{lastannounce} = $NOWTIME;
						$self->{huntlist}->{$huntkey}->{announce_count}++;
					}
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
		
		# walk bucklist backwards
		for(my $i=$cached_best_bucket; $i >= 0; $i--) {
			next unless defined($self->{huntlist}->{$huntkey}->{buckets}->{$i}); # -> Bucket empty
			foreach my $buckref (@{$self->{huntlist}->{$huntkey}->{buckets}->{$i}}) { # Fixme: We should REALLY get the 3 best, not random
				my $lockstate = $self->GetAlphaLock($huntkey,$buckref);
				
				if($lockstate == 1) { # Just freshly locked
					$self->SetQueryType($buckref,$running_qtype);
					$self->UdpWrite({ip=>$buckref->{ip}, port=>$buckref->{port}, cmd=>$self->$running_qtype($huntkey)});
				}
				elsif($lockstate == 0) { # all locks are in use -> we can escape all loops
					goto BUCKWALK_END;
				}
			}
		}
		BUCKWALK_END:
	}
	
	
	# Ping some nodes to check if them are still alive
	$self->AliveHunter();
	# Really remove killed nodes
	$self->RunKillerLoop();
	
	return 0;
}



sub NetworkHandler {
	my($self,$sock,$buffref,$THIS_IP) = @_;
	
	my $THIS_PORT = $sock->peerport();
	my $THIS_BUFF = $$buffref;
	
	if(!$THIS_PORT) {
		$self->warn("Ignoring data from <$sock>, no peerhost");
		return;
	}
	elsif(length($THIS_BUFF) == 0) {
		$self->warn("$THIS_IP : $THIS_PORT sent no data");
		return;
	}
	
	my $btdec = $self->{super}->Tools->BencDecode($THIS_BUFF);
	
	if(ref($btdec) ne "HASH" or !defined($btdec->{t})) {
		$self->debug("Garbage received from $THIS_IP:$THIS_PORT");
		return;
	}
	
		if($btdec->{y} eq 'q') {
			# -> QUERY
			
			# Check if query fulfills basic syntax
			if(length($btdec->{a}->{id}) != SHALEN or $btdec->{a}->{id} eq $self->{my_sha1}) {
				$self->info("$THIS_IP:$THIS_PORT ignoring malformed query");
				return;
			}
			
			
			# Try to add node:
			$self->AddNode({ip=>$THIS_IP, port=>$THIS_PORT, sha1=>$btdec->{a}->{id}});
			
			# -> Requests sent to us
			if($btdec->{q} eq "ping") {
				$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_ping($btdec)});
				$self->debug("$THIS_IP:$THIS_PORT : Pong reply sent");
			}
			elsif($btdec->{q} eq 'find_node' && length($btdec->{a}->{target}) == SHALEN) {
				$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_findnode($btdec)});
				$self->debug("$THIS_IP:$THIS_PORT (find_node): sent kademlia nodes to peer");
			}
			elsif($btdec->{q} eq 'get_peers' && length($btdec->{a}->{info_hash}) == SHALEN) {
				unless( $self->HandleGetPeersCommand($THIS_IP,$THIS_PORT,$btdec) ) { # -> Try to send some peers
					# failed? -> send kademlia nodes
					my $nbuff = $self->GetConcatedNGFSB($btdec->{a}->{info_hash});
					$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_getpeers($btdec)});
					
					$self->debug("$THIS_IP:$THIS_PORT (get_peers) : sent kademlia nodes to peer");
				}
			}
			elsif($btdec->{q} eq 'announce_peer' && length($btdec->{a}->{info_hash}) == SHALEN) {
				
				if( ( ($self->{my_token_1} eq $btdec->{a}->{token}) or ($self->{my_token_2} eq $btdec->{a}->{token}) ) ) {
					$self->{announce}->{$btdec->{a}->{info_hash}}->{$btdec->{a}->{id}} = { ip=>$THIS_IP, port=>$btdec->{a}->{port}, seen=>$self->{super}->Network->GetTime };
					
					if(scalar(keys(%{$self->{announce}})) > MAX_TRACKED_ANNOUNCE) {
						# Too many tracked torrents -> rollback
						delete($self->{announce}->{$btdec->{a}->{info_hash}});
					}
					elsif(scalar(keys(%{$self->{announce}->{$btdec->{a}->{info_hash}}->{$btdec->{a}->{id}}})) > MAX_TRACKED_PEERS) {
						# Too many peers in this torrent -> rollback
						delete($self->{announce}->{$btdec->{a}->{info_hash}}->{$btdec->{a}->{id}});
					}
					else {
						# Report success
						$self->UdpWrite({ip=>$THIS_IP, port=>$THIS_PORT, cmd=>$self->reply_ping($btdec)});
					}
				}
				else {
					$self->warn("$THIS_IP : $THIS_PORT : FIXME: SHOULD SEND TOKENERROR");
				}
				
			}
			else {
				$self->info("Unhandled QueryType $btdec->{q}");
			}
		}
		elsif($btdec->{y} eq "r") {
			# -> Response
			my $peer_shaid = $btdec->{r}->{id};
			my $tr2hash    = $self->tr2hash($btdec->{t}); # Reply is for this SHA1
			
			$self->NormalizeReplyDetails($btdec);
			
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
			
			if(!$self->ExistsNodeHash($peer_shaid)) {
				$self->debug("$THIS_IP:$THIS_PORT (".unpack("H*",$peer_shaid).") sent response to unasked question. no thanks.");
				return;
			}
			elsif(!defined($tr2hash) || length($tr2hash) != SHALEN) {
				$self->debug("$THIS_IP:$THIS_PORT sent invalid hash TR");
				return;
			}
			
			
			$self->SetNodeAsGood({hash=>$peer_shaid,token=>$btdec->{r}->{token}});
			$self->FreeSpecificAlphaLock($tr2hash,$peer_shaid);
			
			my $node_qtype = $self->GetQueryType($self->GetNodeFromHash($peer_shaid));
			
			# Accept 'nodes' if we asked 'anything'. Accept it event without a question while bootstrapping
			if($btdec->{r}->{nodes} && ($node_qtype ne '' or $self->MustBootstrap ) ) {
				my $allnodes = $self->_decodeNodes($btdec->{r}->{nodes});
				my $cbest    = $self->{huntlist}->{$tr2hash}->{bestbuck};
				my $numnodes = 0;
				foreach my $x (@$allnodes) {
					next if length($x->{sha1}) != SHALEN;
					next if !$x->{port} or !$x->{ip}; # Do not add garbage
					next unless defined $self->AddNode($x);
					$numnodes++;
				}
				if( ($cbest < $self->{huntlist}->{$tr2hash}->{bestbuck}) && ($self->GetState($tr2hash) == KSTATE_PEERSEARCH ||
				     $self->GetState($tr2hash) == KSTATE_SEARCH_MYSELF)) {
					$self->TriggerHunt($tr2hash);
					$self->ReleaseAllAlphaLocks($tr2hash);
				}
				# Clean Querytrust:
				$self->SetQueryType($self->GetNodeFromHash($peer_shaid),'');
			}
			# Accept values only as a reply to getpeers:
			if($btdec->{r}->{values} && $node_qtype eq 'command_getpeers') {
				my $all_hosts = $self->_decodeIPs($btdec->{r}->{values});
				my $this_sha  = unpack("H*", $tr2hash);
				$self->debug("$this_sha: new BitTorrent nodes from $THIS_IP:$THIS_PORT (".int(@$all_hosts));
				if($self->GetState($tr2hash) == KSTATE_PEERSEARCH) {
					$self->TriggerHunt($tr2hash);
					$self->SetState($tr2hash,KSTATE_SEARCH_DEADEND);
				}
				
				# Tell bittorrent about new nodes:
				if($self->{bittorrent}->Torrent->ExistsTorrent($this_sha)) {
					$self->{bittorrent}->Torrent->GetTorrent($this_sha)->AddNewPeers(@$all_hosts);
				}
				# Clean Querytrust
				$self->SetQueryType($self->GetNodeFromHash($peer_shaid),'');
			}
		}
		elsif($btdec->{y} eq 'e') {
			# just for debugging:
			#$self->warn("$THIS_IP [$THIS_PORT] \n".Data::Dumper::Dumper($btdec));
		}
		else {
			$self->debug("$THIS_IP:$THIS_PORT: Ignored packet with suspect 'y' tag");
		}
}

sub debug { my($self, $msg) = @_; $self->{super}->debug("Kademlia: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Kademlia: ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("Kademlia: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Kademlia: ".$msg); }



########################################################################
# Reply to get_peers command: true if we sent something, false if we failed
sub HandleGetPeersCommand {
	my($self,$ip,$port,$btdec) = @_;
	
	if(exists($self->{announce}->{$btdec->{a}->{info_hash}})) {
		my @nodes    = ();
		foreach my $rk (List::Util::shuffle(keys(%{$self->{announce}->{$btdec->{a}->{info_hash}}}))) {
			my $r = $self->{announce}->{$btdec->{a}->{info_hash}}->{$rk} or $self->panic;
			push(@nodes, $self->_encodeNode({sha1=>'', ip=>$r->{ip}, port=>$r->{port}}));
			last if int(@nodes) > MAX_TRACKED_SEND;
		}
		$self->UdpWrite({ip=>$ip, port=>$port, cmd=>$self->reply_values($btdec,\@nodes)});
		$self->debug("$ip:$port (get_peers) : sent ".int(@nodes)." BitTorrent nodes to peer");
		return 1;
	}
	else {
		return 0;
	}
}


########################################################################
# Updates on-disk boot list
sub SaveBootNodes {
	my($self) = @_;
	
	my $nref = {};
	my $ncnt = 0;
	
	foreach my $node (values(%{$self->{_addnode}->{hashes}})) {
		if($node->{good} && $node->{rfail} < 1) {
			$nref->{$node->{ip}} = $node->{port};
			last if (++$ncnt >= BOOT_SAVELIMIT);
		}
	}
	
	if($ncnt > 0) {
		$self->{super}->Storage->ClipboardSet($self->GetCbId, $self->{super}->Tools->RefToCBx($nref));
	}
}

########################################################################
# Returns some bootable nodes
sub GetBootNodes {
	my($self) = @_;
	
	my @B   = $self->GetHardcodedBootNodes;
	my @R   = ();
	my $cnt = 0;
	my $ref = $self->{super}->Tools->CBxToRef($self->{super}->Storage->ClipboardGet($self->GetCbId));
	
	foreach my $ip (keys(%$ref)) {
		push(@B,{ip=>$ip, port=>$ref->{$ip}});
	}
	
	foreach my $item (List::Util::shuffle(@B)) {
		push(@R,$item);
		last if ++$cnt >= BOOT_KICKLIMIT;
	}
	return @R;
}


sub CheckCurrentTorrents {
	my($self) = @_;
	my %known_torrents = map { $_ => 1 } $self->{bittorrent}->Torrent->GetTorrents;
	foreach my $hsha1 (keys(%{$self->{huntlist}})) {
		my $up_hsha1 = unpack("H40",$hsha1);
		if($self->GetState($hsha1) == KSTATE_SEARCH_MYSELF) {
			next;
		}
		elsif(delete($known_torrents{$up_hsha1})) {
			if($self->{bittorrent}->Torrent->GetTorrent($up_hsha1)->IsPaused) {
				$self->SetState($hsha1, KSTATE_PAUSED);
			}
			elsif($self->GetState($hsha1) == KSTATE_PAUSED) {
				$self->SetState($hsha1, KSTATE_PEERSEARCH);
			}
		}
		else {
			$self->info("Stopping hunt of $up_hsha1");
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
	for(58..0xFF) {
		if(!defined $self->{trmap}->{chr($_)}) {
			$trn = $_;
			$self->{trmap}->{chr($trn)} = $sha;
			last;
		}
	}
	
	if($trn < 0) {
		$self->warn("No TransactionIDs left, too many loaded torrents!");
		return undef;
	}
	
	$self->{huntlist}->{$sha} = { addtime=>$self->{super}->Network->GetTime, trmap=>chr($trn), state=>($initial_state || KSTATE_PEERSEARCH), announce_count => 0,
	                              bestbuck => 0, lasthunt => 0, deadend => 0, lastannounce => 0, deadend_lastbestbuck => 0};
	
	foreach my $old_sha (keys(%{$self->{_addnode}->{hashes}})) { # populate routing table for new target -> try to add all known nodes
		$self->_inject_node_into_huntbucket($old_sha,$sha);
	}
	return 1;
}


sub _inject_node_into_huntbucket {
	my($self,$new_node,$hunt_node) = @_;
	
	$self->panic("Won't inject non-existent node")          unless $self->ExistsNodeHash($new_node);
	$self->panic("Won't inject into non-existent huntlist") unless defined($self->{huntlist}->{$hunt_node});
	
	my $bucket = int(_GetBucketIndexOf($new_node,$hunt_node));
	if(!defined($self->{huntlist}->{$hunt_node}->{buckets}->{$bucket}) or int(@{$self->{huntlist}->{$hunt_node}->{buckets}->{$bucket}}) < K_BUCKETSIZE) {
		# Add new node to current bucket and fixup bestbuck (if it is better)
		my $nref = $self->GetNodeFromHash($new_node);
		push(@{$self->{huntlist}->{$hunt_node}->{buckets}->{$bucket}}, $nref);
		$nref->{refcount}++;
		$self->{huntlist}->{$hunt_node}->{bestbuck} = $bucket if $bucket >= $self->{huntlist}->{$hunt_node}->{bestbuck}; # Set BestBuck cache
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
			
			if($ref->{refcount} != $self->GetNodeFromHash($ref->{sha1})->{refcount}) {
				$self->panic("Refcount fail: $ref->{refcount}");
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

########################################################################
# Returns hash of given TR
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
	return $self->{super}->Tools->sha1($buff);
}

########################################################################
# Bootstrap from given ip
sub BootFromPeer {
	my($self,$ref) = @_;
	
	$self->{trustlist}->{$ref->{ip}.":".$ref->{port}}++;
	
	$ref->{cmd} = $self->command_findnode(_switchsha($self->{my_sha1}));
	$self->UdpWrite($ref);
	$self->info("Booting using $ref->{ip}:$ref->{port}");
}


########################################################################
# Assemble Udp-Payload and send it
sub UdpWrite {
	my($self,$r) = @_;
	
	$r->{cmd}->{v} = 'BF'.pack("n",_BITFLU_APIVERSION); # Add implementation identification
	my $btcmd = $self->{super}->Tools->BencEncode($r->{cmd});
	$self->{super}->Network->SendUdp($self->{udpsock}, ID=>$self->{topclass}, RemoteIp=>$r->{ip}, Port=>$r->{port}, Data=>$btcmd);
}



########################################################################
# Mark node as killable
sub KillNode {
	my($self,$sha1) = @_;
	$self->panic("Invalid SHA: $sha1") unless $self->ExistsNodeHash($sha1);
	$self->{_killnode}->{$sha1}++;
}

########################################################################
# Add a node to our internal memory-only blacklist
sub BlacklistBadNode {
	my($self,$ref) = @_;
	$self->{super}->Network->BlacklistIp($self->{topclass}, $ref->{ip}, 300);
	return undef;
}

########################################################################
# Check if a node is blacklisted
sub NodeIsBlacklisted {
	my($self,$ref) = @_;
	return $self->{super}->Network->IpIsBlacklisted($self->{topclass}, $ref->{ip});
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
		next if(length($r->{token}) != SHALEN); # Got no token :-(
		$self->warn("Announcing to $r->{ip} $r->{port}  ($r->{good}) , token=".unpack("H*",$r->{token}) );
		my $cmd = {ip=>$r->{ip}, port=>$r->{port}, cmd=>$self->command_announce($sha,$r->{token})};
		$self->UdpWrite($cmd);
		$count++;
	}
	return $count;
}


########################################################################
# Returns the $nodenum nearest nodes
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

# Return concated nearbuck list
sub GetConcatedNGFSB {
	my($self,$target) = @_;
	my $aref = $self->GetNearestGoodFromSelfBuck($target);
	return join('', map( $self->_encodeNode($_), @$aref) );
}

# Ping nodes and kick dead peers
sub AliveHunter {
	my($self) = @_;
	my $NOWTIME = $self->{super}->Network->GetTime;
	if($self->{xping}->{trigger} < $NOWTIME-(K_ALIVEHUNT)) {
		$self->{xping}->{trigger} = $NOWTIME;
		
		my $used_slots = scalar(keys(%{$self->{xping}->{list}}));
		
		while ( $used_slots < K_ALIVEHUNT && (my $r = pop(@{$self->{xping}->{cache}})) ) {
			next unless $self->ExistsNodeHash($r->{sha1}); # node vanished
			if( !exists($self->{xping}->{list}->{$r->{sha1}}) && ($r->{good} == 0 or $r->{lastseen}+300 < $NOWTIME) ) {
				$self->{xping}->{list}->{$r->{sha1}} = 0; # No reference; copy it!
				$used_slots++;
			}
		}
		
		if(scalar(@{$self->{xping}->{cache}}) == 0) {
			# Refresh cache
			$self->debug("Refilling cache with fresh nodes");
			@{$self->{xping}->{cache}} = List::Util::shuffle(values(%{$self->{_addnode}->{hashes}}));
		}
		
		foreach my $sha1 (keys(%{$self->{xping}->{list}})) {
			unless($self->ExistsNodeHash($sha1)) {
				delete $self->{xping}->{list}->{$sha1}; # Node vanished
			}
			else {
				
				if($self->{xping}->{list}->{$sha1} == 0) { # not pinged yet
					$self->{xping}->{list}->{$sha1}++;
				}
				elsif( $self->PunishNode($sha1) ) {
					next; # -> Node got killed. Do not ping it
				}
				my $cmd = $self->command_ping(_switchsha($self->{my_sha1}));
				my $nref= $self->GetNodeFromHash($sha1);
				$self->UdpWrite({ip=>$nref->{ip}, port=>$nref->{port},cmd=>$cmd});
			}
		}
	}
}

########################################################################
# Increase rfail and kill node if it appears to be dead
sub PunishNode {
	my($self,$sha) = @_;
	my $nref = $self->GetNodeFromHash($sha);
	if( ++$nref->{rfail} >= K_MAX_FAILS ) {
		$self->KillNode($sha);
		$self->BlacklistBadNode($nref);
		return 1;
	}
	return 0;
}

########################################################################
# Rotate top-secret token
sub RotateToken {
	my($self) = @_;
	$self->{my_token_2} = $self->{my_token_1};
	$self->{my_token_1} = $self->GetRandomSha1Hash("/dev/urandom") or $self->panic("No random numbers");
}

########################################################################
# Remove stale announce entries
sub AnnounceCleaner {
	my($self) = @_;
	my $deadline = $self->{super}->Network->GetTime-(K_REANNOUNCE);
	foreach my $this_sha1 (keys(%{$self->{announce}})) {
		my $peers_left = 0;
		while(my($this_pid, $this_ref) = each(%{$self->{announce}->{$this_sha1}})) {
			if($this_ref->{seen} < $deadline) {
				delete($self->{announce}->{$this_sha1}->{$this_pid});
			}
			else {
				$peers_left++;
			}
		}
		delete($self->{announce}->{$this_sha1}) if $peers_left == 0; # Drop the sha itself
	}
}

########################################################################
# Requests a LOCK for given $hash using ip-stuff $ref
# Returns '1' if you got a lock
# Returns '-1' if the node was locked
# Returns 0 if you are out of locks
sub GetAlphaLock {
	my($self,$hash,$ref) = @_;
	
	$self->panic("Invalid hash")      if !$self->{huntlist}->{$hash};
	$self->panic("Invalid node hash") if length($ref->{sha1}) != SHALEN;
	my $NOWTIME = $self->{super}->Network->GetTime;
	my $islocked = 0;
	my $isfree   = 0;
	# fixme: loop could use a rewrite and should use LAST
	for my $lockn (1..K_ALPHA) {
		if(!exists($self->{huntlist}->{$hash}->{"lockn_".$lockn})) {
			$isfree = $lockn;
		}
		elsif($self->{huntlist}->{$hash}->{"lockn_".$lockn}->{locktime} <= ($NOWTIME-(K_QUERY_TIMEOUT))) {
			# Remove thisone:
			my $topenalty = $self->{huntlist}->{$hash}->{"lockn_".$lockn}->{sha1} or $self->panic("Lock #$lockn had no sha1");
			delete($self->{huntlist}->{$hash}->{"lockn_".$lockn})                 or $self->panic("Failed to remove lock!");
			$isfree = $lockn;
			
			$self->PunishNode($topenalty) if $self->ExistsNodeHash($topenalty); # node still there
		}
		elsif($self->{huntlist}->{$hash}->{"lockn_".$lockn}->{sha1} eq $ref->{sha1}) {
			$islocked = 1;
		}
	}
	
	if($islocked)  {
		return -1;
	}
	elsif($isfree) {
		$self->{huntlist}->{$hash}->{"lockn_".$isfree}->{sha1} = $ref->{sha1} or $self->panic("Ref has no SHA1");
		$self->{huntlist}->{$hash}->{"lockn_".$isfree}->{locktime} = $NOWTIME;
		return 1;
	}
	else {
		return 0; # No locks free
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
# Pong node
sub reply_ping {
	my($self,$bt) = @_;
	return { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}} };
}

########################################################################
# Send get_nodes:values result to peer
sub reply_values {
	my($self,$bt,$aref_values) = @_;
	return { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, token=>$self->{my_token_1}, values=>$aref_values} };
}


########################################################################
# Assemble a ping request
sub command_ping {
	my($self,$ih) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	return { t=>$tr, y=>'q', q=>'ping', a=>{id=>$self->{my_sha1}} };
}

########################################################################
# Assemble an announce request
sub command_announce {
	my($self,$ih,$key) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	$self->panic("Invalid key: $key") if length($key) != SHALEN;
	return { t=>$tr, y=>'q', q=>'announce_peer', a=>{id=>$self->{my_sha1}, port=>$self->{tcp_port}, info_hash=>$ih, token=>$key} };
}

########################################################################
# Assemble FindNode request
sub command_findnode {
	my($self,$ih) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	return { t=>$tr, y=>'q', q=>'find_node', a=>{id=>$self->{my_sha1}, target=>$ih, want=>[ $self->GetWantKey ] } };
}

########################################################################
# Assemble GetPeers request
sub command_getpeers {
	my($self,$ih) = @_;
	my $tr = $self->{huntlist}->{$ih}->{trmap};
	$self->panic("No tr for $ih") unless defined $tr;
	return { t=>$tr, y=>'q', q=>'get_peers', a=>{id=>$self->{my_sha1}, info_hash=>$ih, want=>[ $self->GetWantKey ] } };
}


########################################################################
# Set status of a KNOWN node to 'good'
sub SetNodeAsGood {
	my($self, $ref) = @_;
	
	my $xid = $ref->{hash};
	if($self->ExistsNodeHash($xid)) {
		my $nref = $self->GetNodeFromHash($xid);
		if($nref->{good} == 0) {
			$self->{_addnode}->{badnodes}--;
			$self->{_addnode}->{goodnodes}++;
			$nref->{good} = 1;
			$nref->{rfail} = 0;
		}
		if(defined($ref->{token}) && length($ref->{token}) == SHALEN) {
			$nref->{token} = $ref->{token};
		}
		$nref->{lastseen} = $self->{super}->Network->GetTime;
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
	$self->panic("No port?!")          if !$ref->{port};
	$self->panic("No IP?")             if !$ref->{ip};
	
	my $NOWTIME = $self->{super}->Network->GetTime;
	
	if($xid eq $self->{my_sha1}) {
		$self->debug("AddNode($self,$ref): Not adding myself!");
		return undef;
	}
	elsif($self->NodeIsBlacklisted($ref)) {
		$self->debug("AddNode($self,$ref): Node is blacklisted, not added");
		return undef;
	}
	
	unless($self->ExistsNodeHash($xid)) {
		# This is a new SHA ID
		$self->{_addnode}->{hashes}->{$xid} = { addtime=>$NOWTIME, lastseen=>$NOWTIME, token=>'', rfail=>0, good=>0, sha1=>$xid , qt=>'',
		                                        refcount => 0, ip=>$ref->{ip}, port=>$ref->{port} };
		
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
# Return node reference from sha1-hash
sub GetNodeFromHash {
	my($self,$hash) = @_;
	return ( $self->{_addnode}->{hashes}->{$hash} or $self->panic("GetNodeFromHash would return undef!") );
}

########################################################################
# Returns true if this hash exists.
sub ExistsNodeHash {
	my($self,$hash) = @_;
	return exists($self->{_addnode}->{hashes}->{$hash});
}

########################################################################
# Set Query type to something
sub SetQueryType {
	my($self,$buckref,$what) = @_;
	return $buckref->{qt} = $what;
}

sub GetQueryType {
	my($self,$buckref) = @_;
	return $buckref->{qt};
}

########################################################################
# Returns true if we have not enough nodes
sub MustBootstrap {
	my($self) = @_;
	return ( $self->{_addnode}->{totalnodes} < MIN_KNODES ? 1 : 0 );
}

########################################################################
# Kill nodes added to _killnode
sub RunKillerLoop {
	my($self) = @_;
	foreach my $xkill (keys(%{$self->{_killnode}})) {
		$self->panic("Cannot kill non-existent node") unless $self->ExistsNodeHash($xkill);
		
		my $nk = $self->GetNodeFromHash($xkill);
		my $refcount = $nk->{refcount};
		foreach my $k (keys(%{$self->{huntlist}})) {
			my $bi = int(_GetBucketIndexOf($k,$xkill)); # bucket of this node in this huntlist
			if(ref($self->{huntlist}->{$k}->{buckets}->{$bi}) eq "ARRAY") {
				my $i    = 0;
				my $bs   = undef;
				my $href = $self->{huntlist}->{$k};
				foreach my $noderef (@{$href->{buckets}->{$bi}}) {
					if($noderef->{sha1} eq $xkill) {
						splice(@{$href->{buckets}->{$bi}},$i,1);
						$refcount--;
						$bs = int(@{$href->{buckets}->{$bi}});
						last;
					}
					$i++;
				}
				
				# check if we must fixup bestbuck-entry
				if(defined($bs) && $bs == 0 && $bi == $href->{bestbuck}) {
					delete($href->{buckets}->{$bi});                                                 # Flush empty bucket index
					my $nn = $self->GetNearestNodes($k,1,0);                                         # get a good node
					$href->{bestbuck} = ( int(@$nn) ? _GetBucketIndexOf($k,$nn->[0]->{sha1}) : 0 );  # Fixup bestbuck
				}
			}
		}
		$self->panic("Invalid refcount: $refcount") if $refcount != 0;
		
		# Fixup statistics:
		$self->{_addnode}->{totalnodes}--;
		if($nk->{good}) { $self->{_addnode}->{goodnodes}--; }
		else            { $self->{_addnode}->{badnodes}--; }
		
		# Remove from memory
		delete($self->{_addnode}->{hashes}->{$xkill});
		delete($self->{xping}->{list}->{$xkill});
	}
	$self->{_killnode} = {};
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

package Bitflu::SourcesBitTorrentKademlia::IPv6;
	use base 'Bitflu::SourcesBitTorrentKademlia';
	
	sub _decodeIPs {
		my($self,$ax) = @_;
		
		my @ref   = ();
		my @nodes = $self->{super}->Tools->DecodeCompactIpV6(join('',@$ax));
		
		foreach my $chunk (@nodes) {
			push(@ref, {ip=>$chunk->{ip}, port=>$chunk->{port}});
		}
		return \@ref;
	}
	
	sub _encodeNode {
		my($self,$r) = @_;
		my $sha    = $r->{sha1};
		my $ip     = $r->{ip};
		my $port   = $r->{port};
		my @ipv6 = $self->{super}->Network->ExpandIpV6($ip);
		
		my $pkt = join('', map(pack("n",$_),(@ipv6,$port)));
		return $sha.$pkt;
	}
	
	sub _decodeNodes {
		my($self,$buff) = @_;
		my @ref = ();
		my $bufflen = length($buff);
		
		for(my $i=0; $i<$bufflen; $i+=38) {
			my($nodeID) = unpack("a20",substr($buff,$i));
			my @sx = $self->{super}->Tools->DecodeCompactIpV6(substr($buff,20,18));
			push(@ref, {ip=>$sx[0]->{ip}, port=>$sx[0]->{port}, sha1=>$nodeID});
		}
		return \@ref;
	}
	
	sub GetV4ngfsb {
		my($self,$target) = @_;
		if( my $v4 = $self->{topclass}->{proto}->{4} ) {
			return $v4->GetConcatedNGFSB($target);
		}
		return '';
	}
	
	########################################################################
	# Move nodes6 -> nodes
	sub NormalizeReplyDetails {
		my($self,$ref) = @_;
		delete($ref->{r}->{nodes}); # not allowed in IPv6 kademlia
		$ref->{r}->{nodes} = delete($ref->{r}->{nodes6}) if exists($ref->{r}->{nodes6});
	}
	
	########################################################################
	# Send find_node result to peer
	sub reply_findnode {
		my($self,$bt) = @_;
		
		my $r    = { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, nodes6=>undef} };;
		my $want = $bt->{a}->{want};
		
		$r->{r}->{nodes}  = $self->GetV4ngfsb($bt->{a}->{target}) if ref($want) eq 'ARRAY' && grep(/^n4$/,@$want);
		$r->{r}->{nodes6} = $self->GetConcatedNGFSB($bt->{a}->{target});
		
		return $r;
	}
	
	########################################################################
	# Send get_nodes:nodes result to peer
	sub reply_getpeers {
		my($self,$bt) = @_;
		
		my $r    = { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, token=>$self->{my_token_1}, nodes6=>undef} };
		my $want = $bt->{a}->{want};
		
		$r->{r}->{nodes}  = $self->GetV4ngfsb($bt->{a}->{info_hash}) if ref($want) eq 'ARRAY' && grep(/^n4$/,@$want);
		$r->{r}->{nodes6} = $self->GetConcatedNGFSB($bt->{a}->{info_hash});
		
		return $r;
	}
	
	########################################################################
	# Returns an IPv6
	sub Resolve {
		my($self,$host) = @_;
		my $xip = $self->{super}->Network->ResolveByProto($host)->{6}->[0];
		return ($self->{super}->Network->IsNativeIPv6($xip) ? $xip : undef);
	}
	
	sub GetHardcodedBootNodes {
		return ( {ip=>'router.bitflu.org', port=>7088}, {ip=>'p6881.router.bitflu.org', port=>6881} );
	}
	
	sub GetCbId {
		return 'kboot6';
	}
	
	sub GetWantKey {
		return 'n6';
	}
	
	sub debug_ {
		my($self,@args) = @_;
		$self->info(@args);
	}
	
1;

package Bitflu::SourcesBitTorrentKademlia::IPv4;
	use base 'Bitflu::SourcesBitTorrentKademlia';
	use strict;
	
	########################################################################
	# Decode Nodes
	sub _decodeNodes {
		my($self,$buff) = @_;
		my @ref = ();
		my $bufflen = length($buff);
		for(my $i=0; $i<$bufflen; $i+=26) {
			my ($nodeID,$a,$b,$c,$d,$port) = unpack("a20CCCCn",substr($buff,$i,26));
			my $IP                         = "$a.$b.$c.$d";
			push(@ref, {ip=>$IP, port=>$port, sha1=>$nodeID});
		}
		return \@ref;
	}
	
	
	
	########################################################################
	# Creates a single NODES encoded entry
	sub _encodeNode {
		my($self,$r) = @_;
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
		my($self,$ax) = @_;
		
		my @ref   = ();
		my @nodes = $self->{super}->Tools->DecodeCompactIp(join('',@$ax));
		
		foreach my $chunk (@nodes) {
			push(@ref, {ip=>$chunk->{ip}, port=>$chunk->{port}});
		}
		return \@ref;
	}
	
	sub GetV6ngfsb {
		my($self,$target) = @_;
		if( my $v6 = $self->{topclass}->{proto}->{6} ) {
			return $v6->GetConcatedNGFSB($target);
		}
		return '';
	}
	
	sub NormalizeReplyDetails {
		# nothing to do for ipv4
	}
	
	########################################################################
	# Send find_node result to peer
	sub reply_findnode {
		my($self,$bt) = @_;
		
		my $r    = { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, nodes=>undef} };;
		my $want = $bt->{a}->{want};
		
		$r->{r}->{nodes}  = $self->GetConcatedNGFSB($bt->{a}->{target});
		$r->{r}->{nodes6} = $self->GetV6ngfsb($bt->{a}->{target}) if ref($want) eq 'ARRAY' && grep(/^n6$/,@$want);
		
		return $r;
	}
	
	########################################################################
	# Send get_nodes:nodes result to peer
	sub reply_getpeers {
		my($self,$bt) = @_;
		
		my $r    = { t=>$bt->{t}, y=>'r', r=>{id=>$self->{my_sha1}, token=>$self->{my_token_1}, nodes=>undef} };
		my $want = $bt->{a}->{want};
		
		$r->{r}->{nodes}  = $self->GetConcatedNGFSB($bt->{a}->{info_hash});
		$r->{r}->{nodes6} = $self->GetV6ngfsb($bt->{a}->{info_hash}) if ref($want) eq 'ARRAY' && grep(/^n6$/,@$want);
		
		return $r;
	}
	
	########################################################################
	# Returns an IPv4
	sub Resolve {
		my($self,$host) = @_;
		my $xip = $self->{super}->Network->ResolveByProto($host)->{4}->[0];
		return ($self->{super}->Network->IsValidIPv4($xip) ? $xip : undef);
	}
	
	sub GetHardcodedBootNodes {
		return ( {ip=>'router.bitflu.org', port=>7088},{ip=>'router.utorrent.com', port=>6881}, {ip=>'router.bittorrent.com', port=>6881},
		         {ip=>'p6881.router.bitflu.org', port=>6881} );
	}
	
	sub GetCbId {
		return 'kboot';
	}
	
	sub GetWantKey {
		return 'n4';
	}
	

1;

