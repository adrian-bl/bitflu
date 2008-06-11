package Bitflu::SourcesBitTorrent;

################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2008 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#
#
# This plugin implements simple BitTorrent tracker (client!) support.
# Note: This plugin does mess with the internals of Bitflu::DownloadBitTorrent !
#       Maybe some kind person offers to rewrite this code mess? :-)
#
################################################################################################

use strict;
use List::Util;
use constant _BITFLU_APIVERSION   => 20080611;
use constant TORRENT_RUN          => 3;   # How often shall we check for work
use constant TRACKER_TIMEOUT      => 35;  # How long do we wait for the tracker to drop the connection
use constant TRACKER_MIN_INTERVAL => 360; # Minimal interval value for Tracker replys
use constant TRACKER_SKEW         => 30;  # Avoid storm at startup

use constant SBT_NOTHING_SENT_YET => 0;   # => 'started' will be the next event sent to the tracker
use constant SBT_SENT_START       => 1;   # => 'completed' will be the next event if we completed just now
use constant SBT_SENT_COMPLETE    => 2;   # => download is done, do not send any events to tracker

use constant PERTORRENT_TRACKERBL => '_trackerbl';

################################################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, bittorrent => undef, lazy_netrun => 0,
	             secret => sprintf("%X", int(rand(0xFFFFFFFF))), next_torrentrun => 0, torrents => {} };
	bless($self,$class);
	
	my $tbl   = $mainclass->Configuration->GetValue('torrent_trackerblacklist');
	unless(defined($tbl)) { $mainclass->Configuration->SetValue('torrent_trackerblacklist', '') }
	
	my $bindto = ($self->{super}->Configuration->GetValue('torrent_bind') || 0); # May be null
	
	# Add a fake socket
	$mainclass->Network->NewTcpListen(ID=>$self, Bind=>$bindto, Port=>0, MaxPeers=>8, Callbacks => {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	$mainclass->AddRunner($self) or $self->panic("Unable to add runner");
	return $self;
}

################################################################################################
# Init plugin
sub init {
	my($self) = @_;
	my $hookit = undef;
	
	# Search DownloadBitTorrent hook:
	foreach my $cc (@{$self->{super}->{_Runners}}) {
		if($cc =~ /^Bitflu::DownloadBitTorrent=/) {
			$hookit = $cc;
		}
	}
	if(defined($hookit)) {
		$self->debug("Using '$hookit' to communicate with BitTorrent plugin.");
		$self->{bittorrent} = $hookit;
		$self->{bittorrent}->{super}->Admin->RegisterCommand('tracker'  , $self, '_Command_Tracker', 'Displays information about tracker',
		   [ [undef, "Usage: tracker queue_id [show|blacklist regexp]"], [undef, "This command displays detailed information about BitTorrent trackers"] ]);
		return 1;
	}
	else {
		$self->panic("Unable to find BitTorrent plugin!");
	}
	die "NOTREACHED";
}


################################################################################################
# Mainsub called by bitflu.pl
sub run {
	my($self) = @_;
	
	my $NOW = $self->{super}->Network->GetTime;
	
	# Do not run too often
	return undef if ($NOW == $self->{lazy_netrun});
	# -> Flush buffers!
	$self->{super}->Network->Run($self);
	$self->{lazy_netrun} = $NOW;
	
	# Nothing to do currently
	return undef if ($NOW < $self->{next_torrentrun});
	$self->{next_torrentrun} = $NOW + TORRENT_RUN;
	
	
	foreach my $loading_torrent ($self->{bittorrent}->Torrent->GetTorrents) {
		my $this_torrent = $self->{bittorrent}->Torrent->GetTorrent($loading_torrent);
		
		if($this_torrent->IsPaused) {
			# Skip paused torrent
		}
		elsif(!defined($self->{torrents}->{$loading_torrent})) {
			# Cache data for new torrent
			my $raw_data = $this_torrent->Storage->GetSetting('_torrent') or next; # No torrent, no trackers anyway
			my $decoded  = Bitflu::DownloadBitTorrent::Bencoding::decode($raw_data);
			my $trackers = [];
			
			if(exists($decoded->{'announce-list'}) && ref($decoded->{'announce-list'}) eq "ARRAY") {
				$trackers = $decoded->{'announce-list'};
			}
			else {
				push(@$trackers, [$decoded->{announce}]);
			}
			
			$self->{torrents}->{$loading_torrent} = { cttlist=>[], cstlist=>[], info_hash=>$loading_torrent, skip_until=>$NOW+int(rand(TRACKER_SKEW)),
			                                          stamp=>$NOW, trackers=>$trackers, tracker_socket=>undef, tracker=>'', timeout_at=>0, last_query=>0 };
		}
		else {
			# Just refresh the stamp
			$self->{torrents}->{$loading_torrent}->{stamp} = $NOW;
		}
	}
	
	# Loop for cached torrents
	foreach my $this_torrent (List::Util::shuffle(keys(%{$self->{torrents}}))) {
		my $obj = $self->{torrents}->{$this_torrent};
		if($obj->{stamp} != $NOW) {
			# Whoops, this torrent vanished from main plugin -> drop it
			delete($self->{torrents}->{$this_torrent});
			next;
		}
		else {
			if($obj->{timeout_at} && $obj->{timeout_at} < $NOW) {
				$self->info("$this_torrent : Tracker '$obj->{tracker}' timed out");
				$self->_Network_Close($obj->{tracker_socket});
				$self->{bittorrent}->{super}->Network->RemoveSocket($self, $obj->{tracker_socket});
			}
			elsif($obj->{skip_until} > $NOW) {
				# Nothing to do.
			}
			else {
				$self->QueryTracker($this_torrent);
			}
		}
	}
}


################################################################################################
# Display information about given Torrents tracker
sub _Command_Tracker {
	my($self,@args) = @_;
	
	my $sha1   = $args[0];
	my $cmd    = $args[1];
	my $value  = $args[2];
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	
	if(defined($sha1)) {
		if(!defined($cmd) or $cmd eq "show") {
			if(exists($self->{torrents}->{$sha1})) {
				my $txr = $self->{torrents}->{$sha1};
				push(@MSG, [3, "Trackers for $sha1"]);
				push(@MSG, [undef, "Next Query           : ".localtime($txr->{skip_until})]);
				push(@MSG, [undef, "Last Query           : ".($txr->{last_query} ? localtime($txr->{last_query}) : 'Never contacted') ]);
				push(@MSG, [($self->{torrents}->{$sha1}->{timeout_at}?2:1), "Waiting for response : ".($txr->{timeout_at}?"Yes":"No")]);
				push(@MSG, [undef, "Current Tracker      : $txr->{tracker}"]);
				
				my $allt = '';
				foreach my $aref (@{$self->{torrents}->{$sha1}->{trackers}}) {
					$allt .= join(';',@$aref)." ";
				}
				push(@MSG, [undef, "All Trackers         : $allt"]);
				push(@MSG, [undef, "Tracker Blacklist    : ".$self->GetTrackerBlacklist($sha1)]);
			}
			else {
				push(@SCRAP, $sha1);
				$NOEXEC .= "$sha1: No such torrent";
			}
		}
		elsif($cmd eq "blacklist") {
			if(my $torrent = $self->{bittorrent}->Torrent->GetTorrent($sha1)) {
				$torrent->Storage->SetSetting(PERTORRENT_TRACKERBL, $value);
				push(@MSG, [1, "$sha1: Tracker blacklist set to '$value'"]);
			}
			else {
				push(@SCRAP, $sha1);
				$NOEXEC .= "$sha1: No such torrent";
			}
		}
		else {
			push(@MSG, [2, "Unknown subcommand '$cmd'"]);
		}
	}
	else {
		$NOEXEC .= "Usage error, type 'help tracker' for more information";
	}
	return({MSG=>\@MSG, SCRAP=>\@SCRAP, NOEXEC=>$NOEXEC});
}



sub GetTrackerBlacklist {
	my($self, $sha1) = @_;
	my $tbl = '';
	if((my $torrent = $self->{bittorrent}->Torrent->GetTorrent($sha1))) {
		$tbl = $torrent->Storage->GetSetting(PERTORRENT_TRACKERBL);
	}
	if(!defined($tbl) || length($tbl) == 0) {
		$tbl = $self->{bittorrent}->{super}->Configuration->GetValue('torrent_trackerblacklist');
	}
	return $tbl;
}


################################################################################################
# Handle data received from network
sub _Network_Data {
	my($self, $socket, $buffref) = @_;
	my $torrent = $self->SocketToTorrent($socket);
	# Torrent for this data does *still* exist, so we are appending the buffer
	if(defined($torrent)) {
		$self->{torrents}->{$torrent}->{tracker_data} .= $$buffref;
	}
}


################################################################################################
# Tracker dropped connection, checkout what we did read so far
sub _Network_Close {
	my($self, $socket) = @_;
	my $torrent = $self->SocketToTorrent($socket)                    or return undef;
	my $tobj    = $self->{bittorrent}->Torrent->GetTorrent($torrent) or return undef;
	my $txr     = $self->{torrents}->{$torrent} or $self->panic("_Close for non-existing torrent $torrent");
	
	my $skiptil        = $txr->{skip_until};
	my $tracker        = $txr->{tracker};
	my $timeout        = $txr->{timeout_at};
	$txr->{timeout_at} = 0;  # Nothing to timeout anymore
	$txr->{tracker}    = ''; # Mark current tracker as broken
	
	my $bencoded = '';
	my $in_body  = 0;
	my @nnodes   = ();
	foreach my $line (split(/\n/,$txr->{tracker_data})) {
		if($in_body == 0 && $line =~ /^\r?$/) { $in_body = 1; }
		elsif($in_body)                       { $bencoded .= $line; }
	}
	
	
	if(length($bencoded) == 0) {
		$self->warn("Tracker $tracker timed out");
		return undef;  #ugly hack
	}
	
	my $decoded = Bitflu::DownloadBitTorrent::Bencoding::decode($bencoded);
	
	
	if(ref($decoded) ne "HASH" or !exists($decoded->{peers})) {
		$self->warn("Tracker $tracker didn't deliver any peers");
		return undef;
	}
	
	if(ref($decoded->{peers}) eq "ARRAY") {
		foreach my $cref (@{$decoded->{peers}}) {
			push(@nnodes , { ip=> $cref->{ip}, port=> $cref->{port}, peer_id=> $cref->{'peer id'} } );
		}
	}
	else {
		@nnodes = $self->DecodeCompactIp($decoded->{peers});
	}
	
	my $new_skiptil = $self->{bittorrent}->{super}->Network->GetTime + $decoded->{interval};
	$txr->{skip_until} = ($new_skiptil>$skiptil?$new_skiptil:$skiptil);
	$txr->{tracker}    = $tracker; # Restore value
	
	$self->AdvanceTrackerEventForHash($torrent); # Skip to next tracker event
	
	my @shuffled   = List::Util::shuffle(@nnodes);
	$self->info("$tracker returned ".int(@nnodes)." peers");
	
	foreach my $aa (@shuffled) {
		$self->{bittorrent}->CreateNewOutgoingConnection($torrent,$aa->{ip}, $aa->{port});
	}
	
}



################################################################################################
# Searches socket for given torrent
sub SocketToTorrent {
	my($self,$socket) = @_;
	$self->panic("No socket for $self->SocketToTorrent() ?") unless defined($socket);
	
	foreach my $torrent (keys(%{$self->{torrents}})) {
		if(defined($self->{torrents}->{$torrent}->{tracker_socket}) && $socket eq $self->{torrents}->{$torrent}->{tracker_socket}) {
			return $torrent;
		}
	}
	return undef;
}



########################################################################
# Decodes Compact IP-Chunks
sub DecodeCompactIp {
	my($self, $compact_list) = @_;
	my @peers = ();
		for(my $i=0;$i<length($compact_list);$i+=6) {
			my $chunk = substr($compact_list, $i, 6);
			my $a    = unpack("C", substr($chunk,0,1));
			my $b    = unpack("C", substr($chunk,1,1));
			my $c    = unpack("C", substr($chunk,2,1));
			my $d    = unpack("C", substr($chunk,3,1));
			my $port = unpack("n", substr($chunk,4,2));
			my $ip = "$a.$b.$c.$d";
			push(@peers, {ip=>$ip, port=>$port, peer_id=>""});
		}
	return @peers;
}


################################################################################################
# Start HTTP-Query to tracker
sub QueryTracker {
	my($self, $this_torrent) = @_;
	
	my $obj = $self->{torrents}->{$this_torrent};
	my $NOW = $self->{bittorrent}->{super}->Network->GetTime;
	
	# Set timeouts even if the query itself fails
	$obj->{skip_until} = $NOW + TRACKER_MIN_INTERVAL;
	$obj->{last_query} = $NOW;
	$obj->{timeout_at} = 0;
	
	# This construct is used to select new trackers
	if(int(@{$obj->{cttlist}}) == 0) {
		# Fillup
		$obj->{cttlist} = deep_copy($obj->{trackers});
	}
	if(int(@{$obj->{cstlist}}) == 0) {
		my @rnd = (List::Util::shuffle(@{shift(@{$obj->{cttlist}})}));
		$obj->{cstlist} = \@rnd;
	}
	unless($obj->{tracker}) {
		$obj->{tracker} = ( shift(@{$obj->{cstlist}}) || '' );
	}
	
	
	if($self->{bittorrent}->{super}->Queue->GetStats($this_torrent)->{clients} < $self->{bittorrent}->{super}->Configuration->GetValue('torrent_maxpeers')) {
		my $rsock = $self->HttpQuery($obj);
		if(defined($rsock)) {
			$self->info("$this_torrent : Using $obj->{tracker}");
			$obj->{tracker_socket} = $rsock;
			$obj->{timeout_at}     = $NOW + TRACKER_TIMEOUT;
			delete($obj->{tracker_data});
		}
		else {
			$self->debug("$this_torrent : Tracker '$obj->{tracker}' not contacted");
			$obj->{tracker} = ''; # Skip to next tracker
		}
	}
	else {
		$self->debug("$this_torrent : No tracker contacted: No need to get any new clients");
	}
}


################################################################################################
# Assemble HTTP-Query and send it to tracker
sub HttpQuery {
	my($self, $obj) = @_;
	
	my ($tracker_host,$tracker_port,$tracker_base) = $obj->{tracker} =~ /^http:\/\/([^\/:]+):?(\d*)\/(.+)$/i;
	return undef unless defined($tracker_host);
	
	$tracker_port       ||= 80;
	my $tracker_blacklist = $self->GetTrackerBlacklist($obj->{info_hash});
	
	if(length($tracker_blacklist) != 0 && $tracker_host =~ /$tracker_blacklist/i) {
		$self->debug("Skipping blacklisted tracker host: $tracker_host");
		$obj->{tracker} = '';
		return undef;
	}
	
	my $stats    = $self->{bittorrent}->{super}->Queue->GetStats($obj->{info_hash});
	my $nextchar = "?";
	   $nextchar = "&" if ($tracker_base =~ /\?/);
	# Create good $key and $peer_id length
	my $key      = _uri_escape(pack("H*",unpack("H40",$self->{secret}.("x" x 20))));
	my $peer_id  = _uri_escape(pack("H*",unpack("H40",$self->{bittorrent}->{CurrentPeerId})));
	
	my $event = $self->GetTrackerEventForHash($obj->{info_hash});
	
	
	my $q  = "GET /".$tracker_base.$nextchar."info_hash="._uri_escape(pack("H*",$obj->{info_hash}));
	   $q .= "&peer_id=".$peer_id;
	   $q .= "&port=".int($self->{bittorrent}->{super}->Configuration->GetValue('torrent_port'));
	   $q .= "&uploaded=".int($stats->{uploaded_bytes});
	   $q .= "&downloaded=".int($stats->{done_bytes});
	   $q .= "&left=".int($stats->{total_bytes}-$stats->{done_bytes});
	   $q .= "&key=".$key;
	   $q .= "&event=$event";
	   $q .= "&compact=1";
	   $q .= " HTTP/1.0\r\n";
	   $q .= "Host: $tracker_host:$tracker_port\r\n\r\n";
	
	my $tsock = $self->{bittorrent}->{super}->Network->NewTcpConnection(ID=>$self, Port=>$tracker_port, Hostname=>$tracker_host, Timeout=>5) or return undef;
	            $self->{bittorrent}->{super}->Network->WriteData($tsock, $q) or $self->panic("Unable to write data to $tsock !");
	return $tsock;
}


sub GetTrackerEventForHash {
	my($self,$hash) = @_;
	my $tobj     = $self->{bittorrent}->Torrent->GetTorrent($hash);
	
	my $current_setting = int($tobj->Storage->GetSetting('_sbt_trackerstat') || SBT_NOTHING_SENT_YET);
	
	if($current_setting == SBT_NOTHING_SENT_YET) {
		return 'started';
	}
	elsif($current_setting == SBT_SENT_START && $tobj->IsComplete) {
		return 'completed';
	}
	else {
		return '';
	}
}


sub AdvanceTrackerEventForHash {
	my($self,$hash) = @_;
	my $tobj     = $self->{bittorrent}->Torrent->GetTorrent($hash);
	
	my $current_setting = $self->GetTrackerEventForHash($hash);
	my $nsetting        = 0;
	
	if($current_setting eq 'started') {
		$nsetting = SBT_SENT_START;
	}
	elsif($current_setting eq 'completed') {
		$nsetting = SBT_SENT_COMPLETE;
	}
	
	$tobj->Storage->SetSetting('_sbt_trackerstat', $nsetting) if $nsetting != 0;
	
}



################################################################################################
# Primitive Escaping
sub _uri_escape {
	my($string) = @_;
	my $esc = undef;
	foreach my $c (split(//,$string)) {
		$esc .= sprintf("%%%02X",ord($c));
	}
	return $esc;
}

################################################################################################
# Stolen from http://www.stonehenge.com/merlyn/UnixReview/col30.html
sub deep_copy {
	my $this = shift;
	if (not ref $this) {
		$this;
	} elsif (ref $this eq "ARRAY") {
		[map deep_copy($_), @$this];
	} elsif (ref $this eq "HASH") {
		+{map { $_ => deep_copy($this->{$_}) } keys %$this};
	} else { die "what type is $_?" }
}


sub debug { my($self, $msg) = @_; $self->{super}->debug("Tracker : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Tracker : ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("Tracker : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Tracker : ".$msg); }

1;
