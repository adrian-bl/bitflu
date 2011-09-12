package Bitflu::DownloadHTTP;
#
# This file is part of 'Bitflu' - (C) 2011 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#



use strict;
use POSIX qw(ceil);

use constant _BITFLU_APIVERSION => 20110508;
use constant HEADER_SIZE_MAX    => 64*1024;     # Size limit for http-headers (64kib)
use constant DEFAULT_CHUNKSIZE  => 1024*1024*8; # chunksize - also the limit for 'dynamic downloads'
use constant ESTABLISH_TIMEOUT  => 10;
use constant HEADER_SENT        => 123;
use constant READ_BODY          => 321;
use constant QUEUE_TYPE         => 'http';
use constant RUN_DELAY          => 6;
use constant QUICK_SCANS        => 3;

##########################################################################
# Registers the HTTP Plugin
sub register {
	my($class, $mainclass) = @_;
	
	my $self = { super=>$mainclass, sockmap=>{}, quick_scan=>QUICK_SCANS };
	bless($self,$class);
	
	# set default value for http_maxthreads
	
	# fixme: do we need maxthreads? it would be nicer to queue this via cron
	
	$mainclass->Configuration->SetValue('http_maxthreads',                       ($mainclass->Configuration->GetValue('http_maxthreads') || 10) );
	$mainclass->Configuration->SetValue('http_autoloadtorrent', 1) unless defined($mainclass->Configuration->GetValue('http_autoloadtorrent')   );
	
	my $main_socket = $mainclass->Network->NewTcpListen(ID=>$self, Port=>0, MaxPeers=>$mainclass->Configuration->GetValue('http_maxthreads'), DownThrottle=>1,
	                                                    Callbacks=>{Data=>'_Network_Data', Close=>'_Network_Close'});
	$mainclass->AddRunner($self);
	return $self;
}

##########################################################################
# Register admin commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('load', $self, '_StartHttpDownload', "Start download of HTTP-URL",
	  [ [undef, "Bitflu can load files via HTTP (like wget)"], [undef, "To start a http download use: 'load http://www.example.com/foo/bar.tgz'"] ] );
	return 1;
}


##########################################################################
# Handles a new 'load' command
sub _StartHttpDownload {
	my($self, @args) = @_;
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	
	foreach my $arg (@args) {
		if(my ($xmode,$xhost,$xport,$xurl) = $arg =~ /^(http|internal\@[^:]+):\/?\/([^\/:]+):?(\d*)\/(.*)$/i) {
			
			$xport ||= 80;
			$xhost   = lc($xhost);
			$xurl    = $self->{super}->Tools->UriEscape($self->{super}->Tools->UriUnescape($xurl));
			$xurl    =~ tr/\///s; $xurl =~ s/^\///; $xurl =~ s/\/$//; # remove duplicate slashes and slashes at the beginning + the end
			my $sid  = $self->{super}->Tools->sha1_hex("http://$xhost:$xport/$xurl");
			
			### FIXME: WE NEED TO HANDLE INTERNAL LINKS (RSS)
			
			if($self->{super}->Storage->OpenStorage($sid)) {
				push(@MSG, [2, "$sid: Download exists in queue ($arg)"]);
			}
			elsif( $self->_SetupStorage($sid, 1024, $xhost, $xport, $xurl) ) { # setup a fake storage
				$self->_PickupDownload($sid);
				push(@MSG, [1, "$sid: http download started"]);
			}
			else { # ->AddItem failed (dl history)
				push(@MSG, [2, "$@"]);
			}
		}
		else {
			push(@SCRAP, $arg);
		}
	}
	
	if(!int(@args)) {
		$NOEXEC = 'Usage: load http://www.example.com';
	}
	
	return({MSG=>\@MSG, SCRAP=>\@SCRAP, NOEXEC=>$NOEXEC});
}


sub run {
	my($self) = @_;
	
	$self->debug("run(): updating downspeed stats for all http downloads");
	
	# update download stats
	foreach my $sm (values(%{$self->{sockmap}})) {
		my $diff         = ($sm->{downspeed} == 0 ? 0 : $sm->{offset} - $sm->{downspeed});
		$sm->{downspeed} = $sm->{offset};
		$self->{super}->Queue->SetStats($sm->{sid}, { speed_download=>int($diff/RUN_DELAY) });
	}
	
	
	goto EXIT_RUN if --$self->{quick_scan};
	
	$self->debug("run(): doing a full httpqueue scan");
	
	$self->{quick_scan} = QUICK_SCANS;
	my $qref            = $self->{super}->Queue;
	my $qlist           = $qref->GetQueueList;
	my $httpq = {};
	
	# get keys of all http downloads
	map({ $httpq->{$_}=1}  keys(%{$qlist->{''.QUEUE_TYPE}}));
	
	foreach my $this_sid (keys(%$httpq)) {
		$self->debug("checking http sid $this_sid");
		if($qref->IsPaused($this_sid)) {
			$self->_KillConnectionOfSid($this_sid); # fixme: commits dynamic downloads
		}
		elsif( !$self->_GetSockmapKeyOfSid($this_sid) ) {
			$self->info("$this_sid: resuming stalled download");
			$self->_PickupDownload($this_sid);
		}
	}
	
	
	EXIT_RUN:
		return RUN_DELAY;
}

##########################################################################
# Called by queue manager: move free piece to inwork and kick start
sub resume_this {
	my($self,$sid) = @_;
	my $so = $self->{super}->Storage->OpenStorage($sid) or $self->panic("could not open $sid !");
	
	# move all free pieces into inwork state:
	map({ $so->SetAsInwork($_) if $so->IsSetAsFree($_) } (0..($so->GetSetting('chunks')-1)));
	# ..and init some fake-stats
	$self->_ResetStats($sid);
}

##########################################################################
# Sets some sane (storage related) stats
sub _ResetStats {
	my($self,$sid) = @_;
	my $so = $self->{super}->Storage->OpenStorage($sid) or $self->panic("could not open $sid !");
	$self->{super}->Queue->InitializeStats($sid);
	
	my ($done_chunks, $done_bytes) = (0,0);
	for(0..$so->GetSetting('chunks')-1) {
		if($so->IsSetAsDone($_)) {
			$done_chunks++;
			$done_bytes += $so->GetSizeOfDonePiece($_);
		}
		else {
			# -> must be inworK. crashes if it was free
			$done_bytes += $so->GetSizeOfInworkPiece($_);
		}
	}
	
	$self->{super}->Queue->SetStats($sid, {total_bytes=>($so->GetSetting('size')*$so->GetSetting('chunks') - $so->GetSetting('overshoot')),
	                                       total_chunks=>$so->GetSetting('chunks'),
	                                       done_bytes=>$done_bytes,
	                                       done_chunks=>$done_chunks,
	                                       });
}


##########################################################################
# check if we have to initiate a new http connection
sub _PickupDownload {
	my($self,$sid) = @_;
	my $so = $self->{super}->Storage->OpenStorage($sid) or $self->panic("could not open $sid !");
	my $qr = $self->{super}->Queue;
	
	$self->debug("pickup $sid");
	if($qr->GetStat($sid,'total_bytes') != $qr->GetStat($sid,'done_bytes') && !$qr->IsPaused($sid)) {
		$self->_InitiateHttpConnection($sid);
	}
}

##########################################################################
# removes an active download
sub cancel_this {
	my($self,$sid) = @_;
	
	$self->debug("$sid: canceling active download");
	
	# terminate all open connections (if any) and remove item from queue
	$self->_KillConnectionOfSid($sid);
	$self->{super}->Queue->RemoveItem($sid);
	
	return undef;
}


##########################################################################
# Send HTTP Request for given SID/SHA value
sub _InitiateHttpConnection {
	my($self,$sid) = @_;
	
	my $so       = $self->{super}->Storage->OpenStorage($sid) or $self->panic;
	my $offset   = $self->{super}->Queue->GetStat($sid,'done_bytes');
	my $new_sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$so->GetSetting('_port'),
	                                                         Hostname=>$so->GetSetting('_host'), Timeout=>ESTABLISH_TIMEOUT);
	return undef if !$new_sock; # -> resolver/ulimit fail
	
	# prepare http header
	my $wdata = "GET /".$so->GetSetting('_url')." HTTP/1.0\r\n";
	   $wdata .= "Host: ".$so->GetSetting('_host')."\r\n";
	   $wdata .= "Range: bytes=".$offset."-\r\n";
	   $wdata .= "User-Agent: Bitflu ".$self->{super}->GetVersionString."\r\n";
	   $wdata .= "Connection: Close\r\n\r\n";
	
	
	$self->debug("$sid: sending http header via socket <$new_sock>");
	
	
	# are we able to kill an existing connection for $sid? if yes -> something is VERY wrong!
	$self->panic("$sid already had a running download!") if $self->_KillConnectionOfSid($sid);
	
	$self->{super}->Network->WriteDataNow($new_sock,$wdata);
	$self->{sockmap}->{$new_sock} = { sid=>$sid, sock=>$new_sock, status=>HEADER_SENT, so=>undef, piggyback=>'', offset=>0, size=>0, free=>0, downspeed=>0 };

}

##########################################################################
# Handle received data
sub _Network_Data {
	my($self, $socket, $bref) = @_;
	my $sm = $self->{sockmap}->{$socket} or $self->panic("No sockmap info for $socket !");
	
	if($sm->{status} == HEADER_SENT && length($sm->{piggyback}) < HEADER_SIZE_MAX) {
		$sm->{piggyback} .= $$bref; # header could be sent in multiple reads
		my $hbytes        = 0;      # size of header
		my $clen          = 0;      # content length
		my $coff          = 0;      # offset
		
		$self->debug("---- parsing http header received by <$socket> ---");
		
		foreach my $line (split(/\r\n/,$sm->{piggyback})) {
			
			$self->debug(sprintf("%s header: <%4d> **%s**", $sm->{sid}, length($line), $line));
			
			$hbytes += 2+length($line); # line + \r\n
			if(length($line) == 0) {
				# We hit the end of the HTTP-Header:
				# We now have to add non-header data back to the ref
				# ..switch state and change the storage size (if not correct)
				
				my $x         = substr($sm->{piggyback},$hbytes);
				$bref         = \$x;
				$sm->{status} = READ_BODY;
				$sm->{offset} = $coff;
				$sm->{size}   = $coff+$clen;
				$sm->{so}     = $self->_FixupStorage($sm->{sid}, $sm->{size});
				$sm->{free}   = $self->{super}->Queue->GetStat($sm->{sid}, 'total_bytes') - $sm->{offset};
				
				if($sm->{offset} != $self->{super}->Queue->GetStat($sm->{sid}, 'done_bytes')) {
					$self->warn("$sm->{sid}: unexpected offset ($sm->{offset}), restarting download from zero");
					$self->_KillConnectionOfSid($sm->{sid}) or $self->panic;
					
					# abuse _FixupStorage to clean everything:
					map({$self->_FixupStorage($sm->{sid}, $sm->{size}+$_)} qw(1 0));
					
					return;
				}
				
				
				# fixme: we should check for insane values in $coff+clen
				$self->debug("$sm->{sid}: header read : offset is at $coff , content-length is $clen, free space is $sm->{free}");
				last; # rest of piggyback would belong to body
			}
			elsif($line =~ /^Content-Length: (\d+)$/) {
				$clen = $1;
			}
			elsif($line =~ /^Content-Range: bytes (\d+)-/) {
				$coff = $1;
			}
		}
		
	}
	
	RELOOP:
	# must be after HEADER_SENT if()
	if($sm->{status} == READ_BODY) {
		my $dlen = length($$bref);
		
		if($sm->{free}-$dlen >= 0) {
			
			my $chunksize    = $sm->{so}->GetSetting('size');               # free space per piece (lower for the latest, but the previous if() wouldn't match)
			my $piece_to_use = int($sm->{offset}/$chunksize);               # piece we have to use
			my $piece_offset = $sm->{offset} - $piece_to_use*$chunksize;    # offset within the piece itself
			my $piece_free   = $chunksize - $piece_offset;                  # free space of this piece
			my $can_write    = ($dlen > $piece_free ? $piece_free : $dlen); # how much data are we going to write
			
			$sm->{so}->WriteData(Chunk=>$piece_to_use, Offset=>$piece_offset, Length=>$can_write, Data=>$bref);
			
			$sm->{offset} += $can_write;
			$sm->{free}   -= $can_write;
			$self->{super}->Queue->SetStats($sm->{sid}, {active_clients=>1, clients=>1, done_bytes=>$sm->{offset}});
			
			if($piece_free-$can_write == 0) {
				$self->warn("$piece_to_use is finished");
				$sm->{so}->SetAsDone($piece_to_use);
				$self->{super}->Queue->SetStats($sm->{sid}, {done_chunks=>$piece_to_use+1});
				goto RELOOP if $dlen > $can_write; # more to write: no need to update $bref ->WriteData did this already
			}
			
		}
		else {
			# whoops! storage would overflow -> kill connection
			$self->warn("$sm->{sid}: received too much data! (free=$sm->{free}), dropping connection with server.");
			$self->_KillConnectionOfSid($sm->{sid}) or $self->panic("expected open socket!");
			return; # the state of $sm will be messed up -> return asap
		}
	}
	
}

sub _Network_Close {
	my($self,$socket) = @_;
	my $sm  = delete($self->{sockmap}->{$socket}) or $self->panic("Could not remove $socket from sockmap: did not exist!");
	my $qr  = $self->{super}->Queue;
	my $sid = $sm->{sid};
	
	if($sm->{status} == READ_BODY) {
		if($qr->GetStat($sid,'done_bytes') == $qr->GetStat($sid,'total_bytes')) {
			$self->debug("$sid: download finished");
		}
		elsif($sm->{size} == 0 && $qr->GetStat($sid,'total_bytes') == DEFAULT_CHUNKSIZE && $qr->GetStat($sid, 'total_chunks') == 1) {
			$self->debug("$sid: dynamic download finished");
			
			# dynamic downloads only have one piece as total_bytes == DEFAULT_CHUNKSIZE
			$sm->{so}->SetAsDone(0) unless $sm->{so}->IsSetAsDone(0);
			
			if($qr->GetStat($sid, 'done_bytes') != $qr->GetStat($sid, 'total_bytes')) {
				# -> need to fixup existing storage
				my $tmp_buff = $sm->{so}->ReadDoneData(Chunk=>0, Offset=>0, Length=>$sm->{so}->GetSizeOfDonePiece(0));
				my $tmp_len  = length($tmp_buff);
				$sm->{so}    = $self->_FixupStorage($sid, $tmp_len); # we will never get multiple pieces
				$sm->{so}->WriteData(Chunk=>0, Offset=>0, Length=>$tmp_len, Data=>\$tmp_buff);
				$sm->{so}->SetAsDone(0);
			}
		}
		# else: -> incomplete download: run() should pick it up later
	}
	else {
		$self->debug("<$socket> dropped in non-body read state - nothing to do");
	}
	
	$self->_ResetStats($sid); # zero-out stats: also sets speed and active_clients to zero
	
	if($qr->GetStat($sid, 'total_chunks') == 1 && $qr->GetStat($sid, 'done_chunks') == 1 &&
	      $self->{super}->Configuration->GetValue('http_autoloadtorrent')) {
		# -> small download: could be a torrent
		$self->_AutoLoadTorrent($sm);
	}

}

##########################################################################
# Terminate open connection of $sid: return 0 if there were none, 1 otherwise
sub _KillConnectionOfSid {
	my($self,$sid) = @_;
	
	my $xsock = $self->_GetSockmapKeyOfSid($sid);
	
	if($xsock) {
		$self->debug("$sid: active socket: <$xsock>, closing down connection");
		$self->_Network_Close($xsock);
		$self->{super}->Network->RemoveSocket($self, $xsock);
		return $xsock;
	}
	return 0;
}

##########################################################################
# Returns the key of $sid - 'false' on error
sub _GetSockmapKeyOfSid {
	my($self,$sid) = @_;
	foreach my $xref (values(%{$self->{sockmap}})) {
		if($xref->{sid} eq $sid) {
			return $xref->{sock};
		}
	}
	return 0;
}


##########################################################################
# Re-Create storage if needed to match the desired value
sub _FixupStorage {
	my($self,$sid,$clen) = @_;
	
	
	my $want_size = ($clen || DEFAULT_CHUNKSIZE);
	my $so        = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Could not open storage $sid");
	my $now_size  = $self->{super}->Queue->GetStat($sid, 'total_bytes');
	
	if($now_size != $want_size) {
		$self->debug("swapping storage: now_size=$now_size != want_size=$want_size");
		my @old = map({$so->GetSetting($_)} qw(_host _port _url));         # save old settings
		$self->{super}->Queue->RemoveItem($sid);                           # remove old item
		$self->{super}->Admin->ExecuteCommand('history', $sid, 'forget');  # ditch it from history
		$so = $self->_SetupStorage($sid,$want_size,@old);                  # and re-add with correct size
		$self->panic("_SetupStorage failed!") unless $so;                  # shouldn't happen
	}
	
	return $so;
}

##########################################################################
# Registers a new storage item and sets the default settings
sub _SetupStorage {
	my($self,$sid,$size,$host,$port,$url) = @_;
	
	my @pathref = split('/', "$host/$url");
	my $dlname  = $pathref[-1] || 'file';
	
	my $chunksize = ($size > DEFAULT_CHUNKSIZE ? DEFAULT_CHUNKSIZE : $size);
	my $numchunks = ceil($size / $chunksize) || 1;  # fixme: what happens with zero-sized downloads?
	my $overshoot = $numchunks*$chunksize - $size;
	
	$self->debug("size=$size, chunksize=$chunksize, numchunks=$numchunks, overshoot=$overshoot");
	$self->debug("pathref=@pathref, name=$dlname");
	
	my $so = $self->{super}->Queue->AddItem(Name=>$dlname, Chunks=>$numchunks, Overshoot=>$overshoot, Size=>$chunksize, Owner=>$self,
	                                        ShaName=>$sid, FileLayout=>[{start=>0, end=>$size, path=>\@pathref}]);
	return undef unless $so;
	$so->SetSetting('type', QUEUE_TYPE) or $self->panic;
	$so->SetSetting('_host',   $host)   or $self->panic;
	$so->SetSetting('_port',   $port)   or $self->panic;
	$so->SetSetting('_url',    $url)    or $self->panic;
	
	map({ $so->SetAsInwork($_) } (0..($numchunks-1))); # set all pieces as inwork
	
	# We've just created a new storage -> stats will be empty so we initialize them right now
	$self->_ResetStats($sid);
	return $so;
}


sub _AutoLoadTorrent {
	my($self,$sm) = @_;
	
	my $tbuff = $sm->{so}->ReadDoneData(Chunk=>0, Offset=>0, Length=>$sm->{so}->GetSizeOfDonePiece(0));
	if($tbuff =~ /^d\d+\:[^:]+[0-9e]\:/) {
		# looks like a torrent...
		my $destfile = $self->{super}->Tools->GetExclusivePath($self->{super}->Configuration->GetValue('autoload_dir'));
		if( open(DEST, ">", $destfile) ) {
			print DEST $tbuff;
			close(DEST);
			$self->{super}->Admin->SendNotify("$sm->{sid}: autoloaded downloaded torrent file");
			$self->{super}->Admin->ExecuteCommand('autoload');
			$self->{super}->Admin->ExecuteCommand('cancel', $sm->{sid});
			return 1;
		}
	}
	return 0;
}

sub debug { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }


1;



