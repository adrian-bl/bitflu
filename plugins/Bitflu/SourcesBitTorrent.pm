package Bitflu::SourcesBitTorrent;

################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
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
use constant TORRENT_RUN          => 3;   # How often shall we check for work
use constant TRACKER_TIMEOUT      => 10;  # How long do we wait for the tracker to drop the connection
use constant TRACKER_MIN_INTERVAL => 360; # Minimal interval value for Tracker replys
use constant TRACKER_SKEW         => 30;  # Avoid storm at startup

use constant SBT_NOTHING_SENT_YET => 0;   # => 'started' will be the next event sent to the tracker
use constant SBT_SENT_START       => 1;   # => 'completed' will be the next event if we completed just now
use constant SBT_SENT_COMPLETE    => 2;   # => download is done, do not send any events to tracker


################################################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, bittorrent => undef, lazy_netrun => 0, next_torrentrun => 0, torrents => {} };
	bless($self,$class);
	
	my $tbl   = $mainclass->Configuration->GetValue('torrent_trackerblacklist');
	
	# Create new torrent trackerkey
	my $key = undef;
	for(1..5) { $key .= sprintf("%X",int(rand(0xFFFFFFFF))); }
	$mainclass->Configuration->SetValue('torrent_trackerkey',$key);
	
	unless(defined($tbl)) { $mainclass->Configuration->SetValue('torrent_trackerblacklist', '') }
	
	
	# Add a fake socket
	$mainclass->Network->NewTcpListen(ID=>$self, Port=>0, MaxPeers=>5);
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
		$self->{bittorrent}->{super}->Admin->RegisterCommand('trackers'  , $self, '_Command_ShowTrackers', 'Displays information about tracker(s)',
		   [ [undef, "Usage: trackers [queue_id_regexp]"], [undef, "This command displays detailed information about BitTorrent trackers"] ]);
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
	$self->{super}->Network->Run($self, {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	$self->{lazy_netrun} = $NOW;
	
	# Nothing to do currently
	return undef if ($NOW < $self->{next_torrentrun});
	$self->{next_torrentrun} = $NOW + TORRENT_RUN;
	
	
	foreach my $loading_torrent ($self->{bittorrent}->Torrent->GetTorrents) {
		unless(defined($self->{torrents}->{$loading_torrent})) {
			$self->debug("Caching data for new torrent $loading_torrent");
			my $raw_data = $self->{bittorrent}->Torrent->GetTorrent($loading_torrent)->Storage->GetSetting('_torrent') or $self->panic("Cannot load raw torrent");
			my $encoded  = Bitflu::DownloadBitTorrent::Bencoding::data2hash($raw_data)->{content};
			if(ref($encoded->{'announce-list'}) eq "ARRAY") {
				# Multitracker!
				$self->{torrents}->{$loading_torrent}->{trackers} = $encoded->{'announce-list'};
			}
			else {
				push(@{$self->{torrents}->{$loading_torrent}->{trackers}}, [$encoded->{announce}]);
			}
			
			$self->{torrents}->{$loading_torrent}->{cttlist}    = []; # CurrentTopTrackerList
			$self->{torrents}->{$loading_torrent}->{cstlist}    = []; # CurrentSubTrackerList
			$self->{torrents}->{$loading_torrent}->{info_hash}  = $loading_torrent;
			$self->{torrents}->{$loading_torrent}->{skip_until} = $NOW+int(rand(TRACKER_SKEW));
		}
		$self->{torrents}->{$loading_torrent}->{stamp} = $NOW;
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
			# Fixme: Was ist, wenn es keinen socket gibt?
				$self->info("$this_torrent : Tracker $obj->{tracker} timed out");
				$self->_Network_Close($obj->{tracker_socket});
				$self->{bittorrent}->{super}->Network->RemoveSocket($self, $obj->{tracker_socket});
				next;
			}
			elsif($obj->{skip_until} > $NOW) {
				next;
			}
			$self->QueryTracker($this_torrent);
		}
	}
}


################################################################################################
# Returns a list of trackers
sub _Command_ShowTrackers {
	my($self,@args) = @_;
	
	my $match = $args[0];
	
	my @A = ([undef,undef]);
	
	foreach my $torrent (keys(%{$self->{torrents}})) {
		next if $torrent !~ /$match/;
		push(@A, [3, $torrent]);
		push(@A, [3, "----------------------------------------"]);;
		push(@A, [undef, "Next Query           : ".gmtime($self->{torrents}->{$torrent}->{skip_until})]);
		push(@A, [undef, "Last Query           : ".gmtime($self->{torrents}->{$torrent}->{last_query})]);
		push(@A, [($self->{torrents}->{$torrent}->{timeout_at}?2:1), "Waiting for response : ".($self->{torrents}->{$torrent}->{timeout_at}?"Yes":"No")]);
		push(@A, [undef, "Current Tracker      : $self->{torrents}->{$torrent}->{tracker}"]);
	}
	
	return({CHAINSTOP=>1, MSG=>\@A});
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
	
	# This data will be changed after contacting the tracker:
	my $skiptil = $self->{torrents}->{$torrent}->{skip_until};
	my $tracker = delete($self->{torrents}->{$torrent}->{tracker})   or $self->panic("No tracker for $torrent !");
	# Deletes the timeout because the connection is closed now
	my $timeout = delete($self->{torrents}->{$torrent}->{timeout_at});
	
	my $bencoded = undef;
	my $in_body  = 0;
	my @nnodes   = ();
	foreach my $line (split(/\n/,$self->{torrents}->{$torrent}->{tracker_data})) {
		if($in_body == 0 && $line =~ /^\r?$/) { $in_body = 1; }
		elsif($in_body)                    { $bencoded .= $line; }
	}
	my $decoded = Bitflu::DownloadBitTorrent::Bencoding::data2hash($bencoded);
	
	if(ref($decoded) ne "HASH" or
	   !defined($decoded->{content}->{peers})) {
		$self->warn("Tracker $tracker failed to deliver new peers : ".ref($decoded->{content}->{peers}));
		return undef;
	}
	
	if(ref($decoded->{content}->{peers}) eq "ARRAY") {
		foreach my $cref (@{$decoded->{content}->{peers}}) {
			push(@nnodes , { ip=> $cref->{ip}, port=> $cref->{port}, peer_id=> $cref->{'peer id'} } );
		}
	}
	else {
		@nnodes = $self->DecodeCompactIp($decoded->{content}->{peers});
	}
	
	my $new_skiptil = $self->{bittorrent}->{super}->Network->GetTime + $decoded->{content}->{interval};
	$self->{torrents}->{$torrent}->{skip_until} = ($new_skiptil>$skiptil?$new_skiptil:$skiptil);
	$self->{torrents}->{$torrent}->{tracker}    = $tracker; # Restore value
	
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
	delete($obj->{timeout_at});
	
	
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
		$obj->{tracker} = shift(@{$obj->{cstlist}});
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
			$self->warn("$this_torrent : Tracker $obj->{tracker} not contacted");
		}
	}
	else {
		$self->warn("$this_torrent : No tracker contacted: No need to get any new clients");
	}
}


################################################################################################
# Assemble HTTP-Query and send it to tracker
sub HttpQuery {
	my($self, $obj) = @_;
	
	my ($tracker_host,$tracker_port,$tracker_base) = $obj->{tracker} =~ /^http:\/\/([^\/:]+):?(\d*)\/(.+)$/i;
	$tracker_port ||= 80;
	return undef unless defined($tracker_host);
	
	my $tracker_blacklist = $self->{bittorrent}->{super}->Configuration->GetValue('torrent_trackerblacklist');
	if(length($tracker_blacklist) != 0 && $tracker_host =~ /$tracker_blacklist/) {
		$self->warn("Skipping blacklisted tracker host: $tracker_host");
		delete($self->{torrents}->{$obj->{info_hash}}->{tracker}) or $self->panic("Unable to remove blacklisted tracker for $obj->{info_hash}");
		return undef;
	}
	
	my $stats    = $self->{bittorrent}->{super}->Queue->GetStats($obj->{info_hash});
	my $nextchar = "?";
	   $nextchar = "&" if ($tracker_base =~ /\?/);
	# Create good $key and $peer_id length
	my $key      = _uri_escape(pack("H*",unpack("H40",$self->{bittorrent}->{super}->Configuration->GetValue('torrent_trackerkey').("x" x 20))));
	my $peer_id  = _uri_escape(pack("H*",unpack("H40",$self->{bittorrent}->{super}->Configuration->GetValue('torrent_peerid').("x" x 20))));
	
	
	my $event = $self->GetTrackerEventForHash($obj->{info_hash});
	
	print "Shall send event: $event\n";
	
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
	
	
	my $tsock = $self->{bittorrent}->{super}->Network->NewTcpConnection(ID=>$self, Port=>$tracker_port, Ipv4=>$tracker_host, Timeout=>5) or return undef;
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


sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }

1;
