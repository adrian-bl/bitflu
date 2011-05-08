package Bitflu::SourcesBitTorrent;

################################################################################################
#
# This file is part of 'Bitflu' - (C) 2008-2011 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#
#
# This plugin implements simple BitTorrent tracker (client!) support.
# Note: This plugin does mess with the internals of Bitflu::DownloadBitTorrent !
#       Maybe some kind person offers to rewrite this code mess? :-)
#
################################################################################################

use strict;
use List::Util;
use constant _BITFLU_APIVERSION   => 20110508;
use constant TORRENT_RUN          => 3;        # How often shall we check for work
use constant TRACKER_TIMEOUT      => 40;       # How long do we wait for the tracker to drop the connection
use constant TRACKER_MIN_INTERVAL => 360;      # Minimal interval value for Tracker replys
use constant TRACKER_SKEW         => 20;       # Avoid storm at startup

use constant SBT_NOTHING_SENT_YET => 0;        # => 'started' will be the next event sent to the tracker
use constant SBT_SENT_START       => 1;        # => 'completed' will be the next event if we completed just now
use constant SBT_SENT_COMPLETE    => 2;        # => download is done, do not send any events to tracker
use constant MAX_ROWFAIL          => 3;        # How often can a tracker fail in a row?
use constant PERTORRENT_TRACKERBL => '_trackerbl';

################################################################################################
# Register this plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, bittorrent => undef, p_tcp=>undef, p_udp=>undef,
	             secret => int(rand(0xFFFFFF)), torrents => {} };
	bless($self,$class);
	
	my $bindto = ($self->{super}->Configuration->GetValue('torrent_bind') || 0); # May be null
	my $cproto = { torrent_trackerblacklist=>'', torrent_tracker_udpport=>6689, torrent_tracker_autoudp=>1 };
	
	foreach my $k (keys(%$cproto)) {
		my $cval = $mainclass->Configuration->GetValue($k);
		unless(defined($cval)) {
			$mainclass->Configuration->SetValue($k, $cproto->{$k});
		}
	}
	
	$mainclass->Configuration->RuntimeLockValue('torrent_tracker_udpport');
	
	
	$self->{p_tcp} = Bitflu::SourcesBitTorrent::TCP->new(_super=>$self, Bind=>$bindto);
	$self->{p_udp} = Bitflu::SourcesBitTorrent::UDP->new(_super=>$self, Bind=>$bindto,
	                                                         Port=>$mainclass->Configuration->GetValue('torrent_tracker_udpport'));
	
	$mainclass->AddRunner($self) or $self->panic("Unable to add runner");
	
	return $self;
}

################################################################################################
# Init plugin
sub init {
	my($self) = @_;
	
	# Search DownloadBitTorrent hook:
	my $hookit = $self->{super}->GetRunnerTarget('Bitflu::DownloadBitTorrent');
	
	if(defined($hookit)) {
		$self->debug("Using '$hookit' to communicate with BitTorrent plugin.");
		$self->{bittorrent} = $hookit;
		$self->{bittorrent}->{super}->Admin->RegisterCommand('tracker'  , $self, '_Command_Tracker', 'Displays information about tracker',
		   [
		   [undef, "Usage: tracker queue_id [show | set TRACKERLIST | blacklist REGEXP ]" ],
		   [undef, ""],
		   [undef, "tracker queue_id show             : Display tracker information"],
		   [undef, "tracker queue_id blacklist \.com   : Skip all trackers matching /\\.com/ (<-- regexp!)"],
		   [undef, "tracker queue_id set TRACKERLIST  : Change trackers of given torrent"],
		   [undef, "tracker queue_id set default      : Revert to default trackerlist (provided by torrent)"],
		   [undef,""],
		   [1    ,"trackerlist example:"],
		   [1    , "',' seperates trackers, '!' forms groups. Also see 'help create_torrent'"],
		   [1    , ""],
		   [1    , "Example: Configure 3 Trackers: t1 , t2 and t3 (t1 and t2 are in the same group)"],
		   [3    , " bitflu> tracker queue_id set t1!t2,t3"],
		   ]);
		return 1;
	}
	else {
		$self->panic("Unable to find BitTorrent plugin!");
	}
}



################################################################################################
# Mainloop
sub run {
	my($self,$NOW) = @_;
	
	my $cnt = 0;
	foreach my $loading_torrent ($self->{bittorrent}->Torrent->GetTorrents) {
		my $this_torrent = $self->{bittorrent}->Torrent->GetTorrent($loading_torrent);
		
		if($this_torrent->IsPaused) {
			# Skip paused torrent
		}
		elsif(!defined($self->{torrents}->{$loading_torrent})) {
			# Cache data for new torrent
			my $trackers = $self->GetTrackers($this_torrent);
			next unless defined $trackers; # -> Not a torrent
			
			my $r4 = { cttlist=>[], cstlist=>[], info_hash=>$loading_torrent, skip_until=>$NOW+($cnt++*TORRENT_RUN), last_query=>0,
			          tracker=>'', rowfail=>0, trackers=>$trackers, waiting=>0, timeout_at=>0, proto=>4 };
			my $r6 = $self->{super}->Tools->DeepCopy($r4);
			$r6->{proto} = 6;
			
			# Remove tracker for unsupported protocols:
			$r4 = undef unless $self->{super}->Network->HaveIPv4;
			$r6 = undef unless $self->{super}->Network->HaveIPv6;
			
			$self->{torrents}->{$loading_torrent} = { v4=>$r4, v6=>$r6, stamp=>$NOW, kill=>0 };
		}
		else {
			# Just refresh the stamp
			$self->{torrents}->{$loading_torrent}->{stamp} = $NOW;
		}
	}
	
	
	# Loop for cached torrents
	foreach my $this_torrent (List::Util::shuffle(keys(%{$self->{torrents}}))) {
		my $xobj = $self->{torrents}->{$this_torrent};
		
		if($xobj->{stamp} != $NOW or $xobj->{kill}) {
			# Whoops, this torrent vanished from main plugin -> drop it
			$self->info("$this_torrent: Aborting tracker requests");
			
			foreach my $obj ($xobj->{v4}, $xobj->{v6}) {
				next unless $obj;
				$self->MarkTrackerAsBroken($obj); # fail and stop current activity (if any)
			}
			
			delete($self->{torrents}->{$this_torrent});
		}
		else {
			foreach my $obj ($xobj->{v4}, $xobj->{v6}) {
				next unless $obj;
				if($obj->{waiting}) { # Tracker has been contacted
					if($obj->{timeout_at} < $NOW) {
						$self->info("$this_torrent: IPv$obj->{proto} tracker '$obj->{tracker}' timed out");
						$self->MarkTrackerAsBroken($obj, Softfail=>1);
						$obj->{skip_until} = $NOW + int(rand(TRACKER_SKEW)); # fast retry
					}
				}
				elsif($obj->{skip_until} > $NOW) {
					# Nothing to do.
				}
				else {
					$self->QueryTracker($obj);
				}
			}
		}
	}
	
	return TORRENT_RUN;
}

################################################################################################
# Read currently configured trackers.
# Fetches a list from torrent if _trackers is empty/does not exist
sub GetTrackers {
	my($self, $tref) = @_;
	
	my $torrent_buffer = $tref->Storage->GetSetting('_torrent')   or return undef; # Not a torrent download
	my $tracker_buffer = ($tref->Storage->GetSetting('_trackers') or '');          # Custom trackerlist. Can be empty
	
	if(length($tracker_buffer) == 0) {
		$self->debug($tref->GetSha1.": no trackerfile: Reading data from raw torrent");
		my $decoded = $self->{super}->Tools->BencDecode($torrent_buffer);
		my $taref   = [];
		
		if(exists($decoded->{'announce-list'}) && ref($decoded->{'announce-list'}) eq "ARRAY") {
			$self->debug($tref->GetSha1." Using announce-list");
			$taref = $self->_LoadAnnounceList($decoded->{'announce-list'});
		}
		
		# -> no tracker-list? try to get a single tracker
		if(int(@$taref) == 0 && $decoded->{announce}) {
			$self->debug($tref->GetSha1." Using announce");
			push(@$taref, [ scalar($decoded->{announce}) ]);
		}
		
		$self->StoreTrackers($tref,$taref);
		$tracker_buffer = ($tref->Storage->GetSetting('_trackers') or ''); # fixme: better handling of non-tracker torrents (-> RemoveSetting ?)
	}
	
	
	$self->debug($tref->GetSha1." loading _trackers file");
	my $trackers = $self->_LoadAnnounceList($self->{super}->Tools->BencDecode($tracker_buffer));
	# must have at least one entry
	push(@$trackers, ['']) if int(@$trackers) == 0;
	
	return $trackers;
}


################################################################################################
# Load (and somewhat verify) an announce list
sub _LoadAnnounceList {
	my($self,$source) = @_;
	my $target = [];
	
	foreach my $this_item (@$source) {
		next if ref($this_item) ne "ARRAY";
		push(@$target, [map( scalar($_||''), @$this_item)] );
	}
	
	return $target;
}



################################################################################################
# Writeout _trackers file
sub StoreTrackers {
	my($self,$tref, $tracker_ref) = @_;
	$self->debug($tref->GetSha1.": storing new trackerlist");
	my $benc = $self->{super}->Tools->BencEncode($tracker_ref);
	$tref->Storage->SetSetting('_trackers',$benc);
	return 1;
}


################################################################################################
# Build trackerlist (if needed) and contact a tracker
sub QueryTracker {
	my($self, $obj) = @_;
	
	my $NOW            = $self->{bittorrent}->{super}->Network->GetTime;
	my $sha1           = $obj->{info_hash};
	$obj->{skip_until} = $NOW + TRACKER_MIN_INTERVAL; # Do not hammer the tracker if it closes the connection quickly
	
	# This construct is used to select new trackers
	if(int(@{$obj->{cttlist}}) == 0) {
		# Fillup
		$obj->{cttlist} = $self->{super}->Tools->DeepCopy($obj->{trackers});
	}
	if(int(@{$obj->{cstlist}}) == 0) {
		my @rnd = (List::Util::shuffle(@{shift(@{$obj->{cttlist}})}));
		my @fixed = ();
		my $autoudp = $self->{super}->Configuration->GetValue('torrent_tracker_autoudp');
		
		foreach my $this_tracker (@rnd) {
			next if !$this_tracker; # Could be empty (funny torrent)
			my($proto,$host,$port,$base) = $self->ParseTrackerUri({tracker=>$this_tracker});
			if($autoudp && $proto eq 'http') { push(@fixed, "udp://$host:$port/$base#bitflu-autoudp") }
			push(@fixed, $this_tracker);
		}
		$obj->{cstlist} = \@fixed;
		$self->debug("cstlist is: \n\t".join("\n\t",@fixed));
	}
	unless($obj->{tracker}) {
		# No selected tracker: get a newone
		$obj->{tracker} = ( shift(@{$obj->{cstlist}}) || '' );  # Grab next tracker
		$self->BlessTracker($obj);                              # Reset fails
		
		if($obj->{tracker} =~ /#bitflu-autoudp$/) {
			# -> only try once if in autoudp mode
			map( $self->MarkTrackerAsBroken($obj,Softfail=>1), (1..MAX_ROWFAIL-1) );
		}
		
	}
	
	$self->ContactCurrentTracker($obj);
}

################################################################################################
# Concact tracker
sub ContactCurrentTracker {
	my($self, $obj) = @_;
	
	my $NOW       = $self->{bittorrent}->{super}->Network->GetTime;
	my $blacklist = $self->GetTrackerBlacklist($obj);
	my $sha1      = $obj->{info_hash} or $self->panic("No info hash");
	my $tracker   = $obj->{tracker};
	
	if(length($tracker) == 0) {
		$self->debug("$sha1: has currently no tracker");
	}
	elsif(length($blacklist) && $tracker =~ /$blacklist/i) {
		$self->debug("$sha1: Skipping blacklisted tracker '$tracker'");
		$self->MarkTrackerAsBroken($obj);
		$obj->{skip_until} = $NOW + int(rand(TRACKER_SKEW));
	}
	else {
		my ($proto)   = $self->ParseTrackerUri($obj);
		# -> Not blacklisted
		if($proto eq 'http') {
			$obj->{timeout_at} = $NOW + TRACKER_TIMEOUT;       # Set response timeout
			$obj->{last_query} = $NOW;                         # Remember last querytime
			$obj->{waiting}    = $self->{p_tcp}->Start($obj);  # Start request via tcp/http
		}
		elsif($proto eq 'udp') {
			$obj->{timeout_at} = $NOW + TRACKER_TIMEOUT;       # Set response timeout
			$obj->{last_query} = $NOW;                         # Remember last querytime
			$obj->{waiting}    = $self->{p_udp}->Start($obj);  # Start request via udp
		}
		else {
			$self->info("$sha1: Protocol of tracker '$tracker' is not supported.");
			$self->MarkTrackerAsBroken($obj);
		}
	}
}

################################################################################################
# Advance to next request and stop in-flight transactions
sub MarkTrackerAsBroken {
	my($self,$obj,%args) = @_;
	
	my $softfail = ($args{Softfail} ? 1 : 0);
	
	$self->debug("MarkTrackerAsBroken($self,$obj), waiting=$obj->{waiting}, softfail=$softfail");
	
	if($obj->{waiting}) {
		$obj->{waiting}->Stop($obj);
		$obj->{waiting} = 0;
	}
	
	if(++$obj->{rowfail} >= MAX_ROWFAIL or !$softfail) {
		$obj->{tracker} = '';
		# rowfail will be reseted while selecting a new tracker
	}
}

################################################################################################
# Mark current tracker as good
sub BlessTracker {
	my($self,$obj) = @_;
	$self->debug("Blessing $obj->{tracker}");
	$obj->{rowfail} = 0;
}

################################################################################################
# Returns the trackerblacklist for given object
sub GetTrackerBlacklist {
	my($self, $obj) = @_;
	my $tbl  = '';
	my $sha1 = $obj->{info_hash} or $self->panic("$obj has no info_hash key!");
	
	if((my $torrent = $self->{bittorrent}->Torrent->GetTorrent($sha1))) {
		$tbl = $torrent->Storage->GetSetting(PERTORRENT_TRACKERBL);
	}
	if(!defined($tbl) || length($tbl) == 0) {
		$tbl = $self->{bittorrent}->{super}->Configuration->GetValue('torrent_trackerblacklist');
	}
	return $tbl;
}

################################################################################################
# Parse an uri
sub ParseTrackerUri {
	my($self, $obj) = @_;
	my ($proto,$host,undef,$port,undef,$base) = $obj->{tracker} =~ /^([^:]+):\/\/([^\/:]+)(:(\d+))?($|\/(.*)$)/;
	$proto  = lc($proto);
	$host   = lc($host);
	$port ||= 80;
	$base ||= '';
	
	$self->debug("ParseTrackerUri($obj->{tracker}) -> proto=$proto, host=$host, port=$port, base=$base");
	
	return($proto,$host,$port,$base);
}

################################################################################################
# Returns current tracker event
sub GetTrackerEvent {
	my($self,$obj) = @_;
	my $sha1     = $obj->{info_hash} or $self->panic("No info_hash?");
	my $tobj     = $self->{bittorrent}->Torrent->GetTorrent($sha1);
	
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

################################################################################################
# Go to next tracker event
sub AdvanceTrackerEvent {
	my($self,$obj) = @_;
	my $sha1            = $obj->{info_hash} or $self->panic("No info_hash?");
	my $tobj            = $self->{bittorrent}->Torrent->GetTorrent($sha1);
	my $current_setting = $self->GetTrackerEvent($obj);
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
# CLI Command
sub _Command_Tracker {
	my($self,@args) = @_;
	
	my $sha1   = $args[0];
	my $cmd    = $args[1];
	my $value  = $args[2];
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	
	if(defined($sha1) && exists($self->{torrents}->{$sha1})) {
		if(!defined($cmd) or $cmd =~ /^(show|list|debug)$/ ) {
			my $xobj = $self->{torrents}->{$sha1};
			my $allt = ''; # List of all trackers
			my $tdbg = ''; # Debug list of all trackers
			foreach my $obj ($xobj->{v4}, $xobj->{v6}) {
				next unless $obj;
				push(@MSG, [3, "Trackers for $sha1 via IPv$obj->{proto}"]);
				push(@MSG, [undef, "Next Query           : ".localtime($obj->{skip_until})]);
				push(@MSG, [undef, "Last Query           : ".($obj->{last_query} ? localtime($obj->{last_query}) : 'Never contacted') ]);
				push(@MSG, [($obj->{waiting}?2:1) ,"Waiting for response : ".($obj->{waiting}?"Yes":"No")]);
				push(@MSG, [undef, "Current Tracker      : $obj->{tracker}"]);
				push(@MSG, [undef, "Fails                : $obj->{rowfail}"]);
				$allt = join(',', map( join('!',@$_), @{$obj->{trackers}})); # Doesn't matter if we use v4 or v6: it's the same anyway...
				$tdbg = Data::Dumper::Dumper($obj->{trackers}) if ($cmd && $cmd eq 'debug');
			}
			
			push(@MSG, [undef, "All Trackers         : $allt"]);
			push(@MSG, [undef, "Tracker Blacklist    : ".$self->GetTrackerBlacklist({info_hash=>$sha1})]); # Ieks: API-Abuse
			if($tdbg) {
				$tdbg =~ s/\n/\r\n/gm;
				push(@MSG, [3, $tdbg]);
			}
		}
		elsif($cmd eq "set" && $value) {
			# , seperates groups. ! seperates trackers
			my @tlist = ();
			if($value eq 'default') {
				@tlist = ([]); # Empty ref -> no tracker
				push(@MSG, [1, "$sha1: Will use default trackerlist from torrent..."]);
			}
			else {
				foreach my $tracker_group (split(/,/,$value)) {
					my @tg_members = split(/!/,$tracker_group);
					push(@tlist,\@tg_members);
				}
			}
			
			$self->StoreTrackers($self->{bittorrent}->Torrent->GetTorrent($sha1), \@tlist);
			$self->{torrents}->{$sha1}->{kill} = 1;
			push(@MSG, [1, "$sha1: Trackerlist updated, reload will happen in a few seconds."]);
		}
		elsif($cmd eq "blacklist") {
			if(my $torrent = $self->{bittorrent}->Torrent->GetTorrent($sha1)) {
				$torrent->Storage->SetSetting(PERTORRENT_TRACKERBL, $value);
				$self->{torrents}->{$sha1}->{kill} = 1;
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
		$NOEXEC .= "Usage error or no such torrent. type 'help tracker' for more information";
	}
	return({MSG=>\@MSG, SCRAP=>\@SCRAP, NOEXEC=>$NOEXEC});
}



sub debug { my($self, $msg) = @_; $self->{super}->debug("Tracker : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Tracker : ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("Tracker : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Tracker : ".$msg); }

1;




################################################################################################



package Bitflu::SourcesBitTorrent::TCP;
	
	use strict;
	use constant TRACKER_MAXPAYLOAD   => 1024*256; # Tracker payload is limited to ~256 KB
	
	################################################################################################
	# Returns a new TCP-Object
	sub new {
		my($class, %args) = @_;
		my $self = { _super=>$args{_super}, super=>$args{_super}->{super}, net=>{bind=>$args{Bind}, port=>0, sock=>undef },
		             sockmap=>{} };
		bless($self,$class);
		
		my $sock = $self->{super}->Network->NewTcpListen(ID=>$self, Bind=>$self->{net}->{bind}, Port=>$self->{net}->{port},
		                                                 DownThrottle=>0,
		                                                 MaxPeers=>8, Callbacks => { Data  =>'_Network_Data',
		                                                                             Close =>'_Network_Close' } );
		$self->{net}->{sock} = $sock;
		return $self;
	}
	
	################################################################################################
	# Starts a new request
	sub Start {
		my($self,$obj) = @_;
		
		my($proto,$host,$port,$base) = $self->{_super}->ParseTrackerUri($obj);
		
		my $sha1     = $obj->{info_hash} or $self->panic("No info_hash?");
		my $stats    = $self->{super}->Queue->GetStats($sha1);
		my $event    = $self->{_super}->GetTrackerEvent($obj);
		my $nextchar = "?";
		   $nextchar = "&" if ($base =~ /\?/);
		
		# Create good $key and $peer_id length
		my $key      = $self->_UriEscape(pack("H40",unpack("H40",$self->{_super}->{secret}.("x" x 20))));
		my $peer_id  = $self->_UriEscape(pack("H40",unpack("H40",$self->{_super}->{bittorrent}->{CurrentPeerId})));
		
		# Assemble HTTP-Request
		my $q  = "GET /".$base.$nextchar."info_hash=".$self->_UriEscape(pack("H40",$obj->{info_hash}));
		   $q .= "&peer_id=".$peer_id;
		   $q .= "&port=".int($self->{super}->Configuration->GetValue('torrent_port'));
		   $q .= "&uploaded=".int($stats->{uploaded_bytes});
		   $q .= "&downloaded=".int($stats->{done_bytes});
		   $q .= "&left=".int($stats->{total_bytes}-$stats->{done_bytes});
		   $q .= "&key=".$key;
		   $q .= "&event=$event" if $event; # BEP-0003 would allow empty events, but some trackers do not like that
		   $q .= "&compact=1";
		   $q .= " HTTP/1.0\r\n";
		   $q .= "User-Agent: Bitflu ".$self->{super}->GetVersionString."\r\n";
		   $q .= "Host: $host:$port\r\n\r\n";
		
		$self->info("$sha1: Contacting $proto://$host:$port/$base via IPv$obj->{proto}...");
		
		my $remote_ip = $self->{super}->Network->ResolveByProto($host)->{$obj->{proto}}->[0];
		
		if($remote_ip) {
			my $tsock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$port, Hostname=>$remote_ip, Timeout=>5);
			if($tsock) {
				$self->{super}->Network->WriteDataNow($tsock, $q) or $self->panic("Unable to write data to $tsock !");
				$self->{sockmap}->{$tsock} = { obj=>$obj, socket=>$tsock, buffer=>'' };
			}
			else {
				# Request will timeout -> tracker marked will be marked as broken
				$self->warn("Failed to create a new connection to $host:$port : $!");
			}
		}
		else {
			$self->debug("Failed to resolve IPv$obj->{proto} record for $host");
		}
		
		return $self;
	}
	
	################################################################################################
	# Append data to buffer (if still active)
	sub _Network_Data {
		my($self,$sock,$buffref,$blen) = @_;
		if(exists($self->{sockmap}->{$sock}) && length($self->{sockmap}->{$sock}->{buffer}) < TRACKER_MAXPAYLOAD) {
			$self->{sockmap}->{$sock}->{buffer} .= ${$buffref}; # append data if socket still active
		}
	}
	
	################################################################################################
	# Connection finished: Parse data and add new peers
	sub _Network_Close {
		my($self,$sock) = @_;
		if(exists($self->{sockmap}->{$sock})) {
			my $smap    = $self->{sockmap}->{$sock};
			my $buffer  = $smap->{buffer};
			my $obj     = $smap->{obj}                     or $self->panic("Missing object!");
			my $sha1    = $obj->{info_hash}                or $self->panic("No info_hash?");
			my $bobj    = $self->{_super}->{bittorrent}    or $self->panic("No BT-Object?");
			my @nnodes  = ();       # NewNodes
			my $hdr_len = 0;        # HeaderLength
			my $decoded = undef;    # Decoded data
			my $failed  = 0;        # Did the tracker fail?
			
			
			# Ditch existing HTTP-Header
			foreach my $line (split(/\n/,$buffer)) {
				$hdr_len += length($line)+1; # 1=\n
				if($line eq "\r")      {
					last; # \n\r found
				}
				elsif(length($line) == 0) {
					# -> Huh? We just got \n\n while reading the header.
					# The tracker seems to speak some sort of brokish-HTTP!
					$self->warn("$obj->{tracker} violates HTTP/1.0! Accepting broken HTTP header :-/");
					last;
				}
			}
			
			if(length($buffer) > $hdr_len) {
				$buffer  = substr($buffer,$hdr_len); # Throws the http header away
				$decoded = $self->{super}->Tools->BencDecode($buffer);
			}
		
			if(ref($decoded) ne "HASH") {
				$self->info("$sha1: received invalid response from IPv$obj->{proto} tracker. (http_header_len=$hdr_len)");
				$failed = 1;
			}
			elsif(exists($decoded->{peers}) && ref($decoded->{peers}) eq "ARRAY") {
				foreach my $cref (@{$decoded->{peers}}) {
					push(@nnodes , { ip=> $cref->{ip}, port=> $cref->{port}, peer_id=> $cref->{'peer id'} } );
				}
			}
			elsif(exists($decoded->{peers6})) {
				@nnodes = $self->{super}->Tools->DecodeCompactIpV6($decoded->{peers6});
			}
			elsif(exists($decoded->{peers})) {
				@nnodes = $self->{super}->Tools->DecodeCompactIp($decoded->{peers});
			}
			
			if(exists($decoded->{'failure reason'})) {
				# avoid messing up the terminal:
				my $clean_fr = $decoded->{'failure reason'};
				$clean_fr =~ tr/\x20-\x7e/?/c;
				$self->warn("$sha1: Error from tracker: $clean_fr");
			}
			
			# Calculate new Skiptime
			my $new_skip = $self->{super}->Network->GetTime + (abs(int($decoded->{interval}||0)));
			my $old_skip = $obj->{skip_until};
			$obj->{skip_until} = ( $new_skip > $old_skip ? $new_skip : $old_skip ); # Set new skip_until time
			$obj->{waiting}    = 0;                                                 # No open transaction
			delete($self->{sockmap}->{$sock}) or $self->panic;                      # Mark socket as down
			
			if($bobj->Torrent->ExistsTorrent($sha1) && !$failed) {
				# Torrent does still exist: add nodes
				$bobj->Torrent->GetTorrent($sha1)->AddNewPeers(List::Util::shuffle(@nnodes));
				$self->{_super}->AdvanceTrackerEvent($obj);
				$self->{_super}->BlessTracker($obj);
				$self->info("$sha1: IPv$obj->{proto} tracker returned ".int(@nnodes)." peers");
			}
			elsif($failed) {
				$self->{_super}->MarkTrackerAsBroken($obj, Softfail=>1)
			}
			
		}
	}
	
	################################################################################################
	# Aborts in-flight transactions
	sub Stop {
		my($self,$obj) = @_;
		
		foreach my $snam (keys(%{$self->{sockmap}})) {
			if($self->{sockmap}->{$snam}->{obj} eq $obj) {
				my $socket = $self->{sockmap}->{$snam}->{socket};
				$self->_Network_Close($socket);                        # cleans sockmap
				$self->{super}->Network->RemoveSocket($self, $socket); # drop connection
			}
		}
		
	}
	
	################################################################################################
	# Primitive Escaping
	sub _UriEscape {
		my($self,$string) = @_;
		my $esc = undef;
		foreach my $c (split(//,$string)) {
			$esc .= sprintf("%%%02X",ord($c));
		}
		return $esc;
	}
	
	sub debug { my($self, $msg) = @_; $self->{_super}->debug($msg); }
	sub info  { my($self, $msg) = @_; $self->{_super}->info($msg);  }
	sub warn  { my($self, $msg) = @_; $self->{_super}->warn($msg);  }
	sub panic { my($self, $msg) = @_; $self->{_super}->panic($msg); }

1;





package Bitflu::SourcesBitTorrent::UDP;
	use constant OP_CONNECT   => 0;  # Connection request
	use constant OP_ANNOUNCE  => 1;  # IPv4 announce
	use constant OP_ERROR     => 3;  # Error (only returned from tracker)
	use constant OP_ANNOUNCE6 => 4;  # IPv6 announce
	################################################################################################
	# Creates a new UDP object
	sub new {
		my($class, %args) = @_;
		my $self = { _super=>$args{_super}, super=>$args{_super}->{super}, net=>{bind=>$args{Bind}, port=>$args{Port},
		             sock=>undef }, tmap=>{}, ccache=>[{t=>0},{t=>0},{t=>0},{t=>0}] };
		bless($self,$class);
		
		my $sock = $self->{super}->Network->NewUdpListen(ID=>$self, Bind=>$self->{net}->{bind}, Port=>$self->{net}->{port},
		                                                            Callbacks => {  Data  =>'_Network_Data' } );
		$self->{net}->{sock} = $sock or $self->panic("Failed to bind to $self->{net}->{bind}:$self->{net}->{port}: $!");
		return $self;
	}
	
	################################################################################################
	# Send a connect() request to current tracker
	sub Start {
		my($self,$obj) = @_;
		my $sha1                     = $obj->{info_hash};                                                    # Info Hash
		my($proto,$host,$port,$base) = $self->{_super}->ParseTrackerUri($obj);                               # Parsed Tracker URI
		my $ip                       = $self->{super}->Network->ResolveByProto($host)->{$obj->{proto}}->[0]; # IP Addr
		my $tid                      = _GetFreeTxId();                                                       # Obtain free Transaction ID
		
		
		# Creates a new TransactionMap (tx) Object:
		my $tx_obj = $self->{tmap}->{$tid} = { id=>$tid, obj => $obj, ip=>$ip, port=>$port, trackerid=>"IPv$obj->{proto}://$host:$port" };
		
		if($ip && $port) {
			# -> Tracker is resolveable
			my $con_id = $self->_GetConnectionId($tx_obj);                          # Do we have a connection id for this tracker?
			if(defined($con_id)) { $self->_WriteAnnounceRequest($tx_obj,$con_id); } # Yes -> Send an announce request
			else                 { $self->_WriteConnectionRequest($tx_obj);       } # No  -> Obtain a new connection_id first
		}
		return $self;
	}
	
	
	################################################################################################
	# Find a random transaction id
	# $tid will be 'something' if this loop ends.
	# This isn't such a big problem: we will just add wrong ips to
	# the a wrong peer (this will result in broken connections..)
	sub _GetFreeTxId {
		my($self) = @_;
		my $tid = 0;
		for(0..255){
			$tid = 1+int(rand(0xFFFFFE));
			last if !exists($self->{tmap}->{$tid});
		}
		return $tid;
	}
	
	
	################################################################################################
	# Changes the id of given tx_obj
	sub _ChangeTransactionId {
		my($self,$tx_obj) = @_;
		
		my $new_id = _GetFreeTxId();
		my $old_id = $tx_obj->{id}       or $self->panic("\$tx_obj has no id!");
		delete($self->{tmap}->{$old_id}) or $self->panic("Could not delete $old_id from tmap!");
		$tx_obj->{id}            = $new_id; # Fixup internal id
		$self->{tmap}->{$new_id} = $tx_obj; # Store in hash
		return $new_id;                     # Return new id
	}
	
	################################################################################################
	# Invalidate transaction of $obj
	sub Stop {
		my($self, $obj) = @_;
		foreach my $trans_id (keys(%{$self->{tmap}})) {
			my $t_obj = $self->{tmap}->{$trans_id}->{obj};
			if($t_obj eq $obj) {
				delete($self->{tmap}->{$trans_id});
				last;
			}
		}
	}
	
	################################################################################################
	# Returns true if torrent still exists in queue
	sub _TorrentExists {
		my($self, $obj) = @_;
		return $self->{_super}->{bittorrent}->Torrent->ExistsTorrent($obj->{info_hash});
	}
	
	################################################################################################
	# Send a connection request to given tracker
	sub _WriteConnectionRequest {
		my($self,$tx_obj) = @_;
		$self->info("$tx_obj->{obj}->{info_hash}: Validating connection to IPv$tx_obj->{obj}->{proto} tracker $tx_obj->{obj}->{tracker}");
		my $payload = pack("H16", "0000041727101980").pack("NN",OP_CONNECT,$tx_obj->{id});
		$self->{super}->Network->SendUdp($self->{net}->{sock}, ID=>$self, RemoteIp=>$tx_obj->{ip}, Port=>$tx_obj->{port}, Data=>$payload);
	}
	
	################################################################################################
	# Send an announce request (=request peers)
	sub _WriteAnnounceRequest {
		my($self,$tx_obj,$con_id) = @_;
		
		my $obj    = $tx_obj->{obj};                             # Tracker object
		my $sha1   = $obj->{info_hash};                          # Current info_hash
		my $btobj  = $self->{_super}->{bittorrent};              # BitTorrent object
		
		if($self->_TorrentExists($obj)) {
			$self->info("$sha1: Requesting new peers from $obj->{tracker}");
			my $t_port  = int($self->{super}->Configuration->GetValue('torrent_port'));
			my $t_key   = $self->{_super}->{secret};
			my $t_pid   = $btobj->{CurrentPeerId};
			my $t_stats = $self->{super}->Queue->GetStats($sha1);
			my $t_estr  = $self->{_super}->GetTrackerEvent($obj);
			my $t_enum  = undef;
			my $opcode  = OP_ANNOUNCE;
			my $ipsize  = "N";
			$t_enum     = ($t_estr eq 'started' ? 2 : ($t_estr eq 'completed' ? 1 : 0 ) );
			
			
			if(0&&$self->{super}->Network->IsNativeIPv6($tx_obj->{ip})) { # Disabled -> Not implemented in opentracker (yet?)
				$self->warn("Using IPv6 announce to $tx_obj->{ip}");
				$opcode = OP_ANNOUNCE6;
				$ipsize = "H32";
			}
			
			
			my $pkt  = pack("H16NN",$con_id,$opcode,$tx_obj->{id});                     # ConnectionId, Opcode, TransactionId
			   $pkt .= pack("H40",$sha1).$t_pid;                                        # info_hash, peer-id (always 20)
			   $pkt .= pack("NN",0,$t_stats->{done_bytes});                             # Downloaded
			   $pkt .= pack("NN",0,($t_stats->{total_bytes}-$t_stats->{done_bytes}));   # Bytes left
			   $pkt .= pack("NN",0,$t_stats->{uploaded_bytes});                         # Uploaded data
			   $pkt .= pack("N",$t_enum);                                               # Event
			   $pkt .= pack($ipsize,0);                                                 # IP(0)
			   $pkt .= pack("NN",$t_key,50);                                            # Secret, NumWant(50)
			   $pkt .= pack("n",$t_port);                                               # Port used by BitTorrent
			$self->{super}->Network->SendUdp($self->{net}->{sock}, ID=>$self, RemoteIp=>$tx_obj->{ip},
			                                                       Port=>$tx_obj->{port}, Data=>$pkt);
		}
	}
	
	################################################################################################
	# Stores a connection id in cache
	sub _CacheConnectionId {
		my($self,$tx_obj,$con_id) = @_;
		my $trackerid = $tx_obj->{trackerid} or $self->panic;
		
		$self->panic("$trackerid HAS a cached connection id!") if defined($self->_GetConnectionId($tx_obj));
		shift(@{$self->{ccache}});
		push(@{$self->{ccache}}, {t=>$self->{super}->Network->GetTime, trackerid=>$trackerid, id=>$con_id});
	}
	
	################################################################################################
	# Fetches a connection-id from cache, return undef on cache-miss
	sub _GetConnectionId {
		my($self,$tx_obj) = @_;
		
		my $ttl = $self->{super}->Network->GetTime-60;  # BEP-15 limits the ttl to 60 seconds
		
		foreach my $cc (@{$self->{ccache}}) {
			if($cc->{t} >= $ttl && $cc->{trackerid} eq $tx_obj->{trackerid}) {
				return $cc->{id};
			}
		}
		return undef;
	}
	
	################################################################################################
	# Handles incoming udp data
	sub _Network_Data {
		my($self,$sock,$buffref) = @_;
		
		my $buffer  = ${$buffref};
		my $bufflen = length($buffer);
		
		if($bufflen >= 16) {
			my($action,$trans_id,$con_id) = unpack("NNH16",$buffer); # Parse udp 'header'
			
			if(exists($self->{tmap}->{$trans_id})) {
				# -> We got an 'open' transaction
				
				my $tx_obj = $self->{tmap}->{$trans_id};             # Transaction object
				my $obj    = $tx_obj->{obj};                         # Tracker object
				my $sha1   = $obj->{info_hash};                      # Current info_hash
				my $btobj  = $self->{_super}->{bittorrent};          # BitTorrent object
				my $NOW    = $self->{super}->Network->GetTime;       # Current timestamp
				
				$obj->{waiting} or $self->panic("$trans_id was in non-wait state?!"); # paranoia check
				
				if($action == OP_CONNECT && !defined($self->_GetConnectionId($tx_obj))) {
					# -> Connect response received. send an announce request
					$self->_CacheConnectionId($tx_obj,$con_id);
					$self->_ChangeTransactionId($tx_obj);
					$self->_WriteAnnounceRequest($tx_obj,$con_id);
				}
				elsif($action == OP_ANNOUNCE && $bufflen >= 20 && $self->_TorrentExists($obj)) {
					# -> Announce-Response for existing torrent
					
					my(undef,undef,$interval,$peercount,$seeders) = unpack("NNNNN",$buffer);
					
					$self->{_super}->AdvanceTrackerEvent($obj); # Mark current event as 'sent'
					$self->{_super}->BlessTracker($obj);        # Mark current tracker as 'alive'
					
					# Parse and add nodes
					my @iplist = $self->{super}->Tools->DecodeCompactIp(substr($buffer,20));
					$btobj->Torrent->GetTorrent($sha1)->AddNewPeers(List::Util::shuffle(@iplist));
					
					my $new_skip = $NOW + (abs(int($interval||0)));
					my $old_skip = $obj->{skip_until};
					$obj->{skip_until} = ( $new_skip > $old_skip ? $new_skip : $old_skip ); # Set new skip_until time
					$obj->{waiting}    = 0;                                                 # No open transaction
					$self->Stop($obj);                                                      # Mark request as completed (invalidate tmap entry)
					
					$self->info("$sha1: Received ".int(@iplist)." peers (info: proto=IPv$obj->{proto} peers=$peercount seeders=$seeders)");
				}
				elsif($action == OP_ERROR) {
					# We will timeout after 40 seconds and retry
					$self->info("$sha1: Tracker returned an error");
				}
				else {
					$self->debug("Ignoring udp-packet with length=$bufflen, action=$action"); # Could be a late connection-id response
				}
			}
			else {
				$self->info("Received udp-packet with invalid transaction-id ($trans_id), dropping data");
			}
		}
	}
	
	sub debug { my($self, $msg) = @_; $self->{_super}->debug($msg); }
	sub info  { my($self, $msg) = @_; $self->{_super}->info($msg);  }
	sub warn  { my($self, $msg) = @_; $self->{_super}->warn($msg);  }
	sub panic { my($self, $msg) = @_; $self->{_super}->panic($msg); }
1;
