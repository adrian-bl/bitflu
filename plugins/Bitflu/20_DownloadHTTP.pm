package Bitflu::DownloadHTTP;
#
# This file is part of 'Bitflu' - (C) 2011 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#



use strict;
use constant _BITFLU_APIVERSION => 20110508;
use constant HEADER_SIZE_MAX    => 64*1024;     # Size limit for http-headers (64kib)
use constant STORAGE_SIZE_DUMMY => 1024*1024*5; # 5mb for 'dynamic' downloads
use constant ESTABLISH_TIMEOUT  => 10;
use constant HEADER_SENT        => 123;
use constant READ_BODY          => 321;


##########################################################################
# Registers the HTTP Plugin
sub register {
	my($class, $mainclass) = @_;
	
	my $self = { super=>$mainclass, sockmap=>{} };
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
			
			my $sid = $self->{super}->Tools->sha1_hex("http://$xhost:$xport/$xurl");
			
			### FIXME: WE NEED TO HANDLE INTERNAL LINKS (RSS)
			
			if($self->{super}->Storage->OpenStorage($sid)) {
				push(@MSG, [2, "$sid: Download exists in queue ($arg)"]);
			}
			elsif( $self->_SetupStorage($sid, 1024, $xhost, $xport, $xurl) ) { # setup a fake storage
				$self->resume_this($sid);
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
	$self->warn("Running!");
	return 5;
}


##########################################################################
# check if we have to initiate a new http connection
sub resume_this {
	my($self,$sid) = @_;
	my $so = $self->{super}->Storage->OpenStorage($sid) or $self->panic("could not open $sid !");
	
	if($so->IsSetAsFree(0)) {
		$self->debug("$sid: piece 0 was free -> setting inwork status");
		$so->SetAsInwork(0)
	}
	
	# Setup some initial stats
	$self->{super}->Queue->InitializeStats($sid);
	$self->{super}->Queue->SetStats($sid, {total_bytes=>$so->GetSetting('size'), total_chunks=>1});
	
	if($so->IsSetAsInwork(0)) {
		# -> not finished: try to initiate a new http connection
		$self->_InitiateHttpConnection($sid);
	}
	else {
		# -> download seems to be completed: update stats
		$self->{super}->Queue->SetStats($sid, {done_chunks=>1, done_bytes=>$so->GetSizeOfDonePiece(0)});
		$self->debug("$sid: download was finished - nothing to resume");
	}
}

##########################################################################
# removes an active download
sub cancel_this {
	my($self,$sid) = @_;
	
	$self->debug("$sid: canceling active download");
	
	# terminate all open connections (if any) and remove item from queue
	$self->_KillConnectionsOfSid($sid);
	$self->{super}->Queue->RemoveItem($sid);
	
	return undef;
}


##########################################################################
# Send HTTP Request for given SID/SHA value
sub _InitiateHttpConnection {
	my($self,$sid) = @_;
	
	my $so       = $self->{super}->Storage->OpenStorage($sid) or $self->panic;
	my $offset   = $so->GetSizeOfInworkPiece(0);
	my $new_sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$so->GetSetting('_port'),
	                                                         Hostname=>$so->GetSetting('_host'), Timeout=>ESTABLISH_TIMEOUT);
	
	# prepare http header
	my $wdata = "GET /".$so->GetSetting('_url')." HTTP/1.0\r\n";
	   $wdata .= "Host: ".$so->GetSetting('_host')."\r\n";
	   $wdata .= "Range: bytes=".$offset."-\r\n";
	   $wdata .= "User-Agent: Bitflu ".$self->{super}->GetVersionString."\r\n";
	   $wdata .= "Connection: Close\r\n\r\n";
	
	
	$self->debug("$sid: sending http header via socket <$new_sock>");
	
	$self->{super}->Network->WriteDataNow($new_sock,$wdata);
	$self->{sockmap}->{$new_sock} = { sid=>$sid, sock=>$new_sock, status=>HEADER_SENT, so=>undef, piggyback=>'', offset=>0, size=>0, free=>0 };
	
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
				$sm->{offset} = $coff; # fixme: if $coff != getsizeofinworkpiece(0) -> truncate 0 and re-do the request
				$sm->{size}   = $coff+$clen;
				$sm->{so}     = $self->_FixupStorage($sm->{sid}, $sm->{size});
				$sm->{free}   = $sm->{so}->GetSetting('size') - $sm->{offset};
				
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
	
	# must be after HEADER_SENT if()
	if($sm->{status} == READ_BODY) {
		my $dlen = length($$bref);
		$sm->{free} -= $dlen;
		
		if($sm->{free} >= 0) {
			$sm->{so}->WriteData(Chunk=>0, Offset=>$sm->{offset}, Length=>$dlen, Data=>$bref);
			$sm->{offset} += $dlen;
			$self->{super}->Queue->SetStats($sm->{sid}, {done_bytes=>$sm->{offset}, active_clients=>1});
		}
		else {
			# whoops! storage would overflow -> kill connection
			$self->warn("$sm->{sid}: received too much data! (free=$sm->{free}), dropping connection with server.");
			$self->_KillConnectionsOfSid($sm->{sid}) or $self->panic("expected open socket!");
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
		if($qr->GetStat($sid,'total_bytes') == $qr->GetStat($sid,'done_bytes')) {
			$self->debug("$sid: download finished");
			$sm->{so}->SetAsDone(0);   # mark piece als done
			$self->resume_this($sid);  # and 'resume' it: this will just update the stats
		}
		elsif($sm->{size} == 0) {
			my $dynamic_size = $sm->{so}->GetSizeOfInworkPiece(0);
			my $dynamic_cpy  = $sm->{so}->ReadInworkData(Chunk=>0, Offset=>0, Length=>$dynamic_size);
			
			$self->debug("$sid: dynamic download finished: size=$dynamic_size");
			
			$sm->{so} = $self->_FixupStorage($sid, $dynamic_size);
			$sm->{so}->WriteData(Chunk=>0, Offset=>0, Length=>$dynamic_size, Data=>\$dynamic_cpy);
			$sm->{so}->SetAsDone(0);
			$self->resume_this($sid);
		}
		# else: -> incomplete download: run() should pick it up later
	}
	else {
		$self->debug("<$socket> dropped in non-body read state - nothing to do");
	}
	
}

##########################################################################
# Terminate open connection of $sid: return 0 if there were none, 1 otherwise
sub _KillConnectionsOfSid {
	my($self,$sid) = @_;
	
	foreach my $xref (values(%{$self->{sockmap}})) {
		if($xref->{sid} eq $sid) {
			my $xsock = $xref->{sock};
			$self->debug("$sid: active socket: <$xsock>, closing down connection");
			$self->_Network_Close($xsock);
			$self->{super}->Network->RemoveSocket($self, $xsock);
			return 1;
		}
	}
	return 0;
}

##########################################################################
# Re-Create storage if needed to match the desired value
sub _FixupStorage {
	my($self,$sid,$clen) = @_;
	
	
	my $want_size = ($clen || STORAGE_SIZE_DUMMY);
	my $so        = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Could not open storage $sid");
	my $now_size  = $so->GetSetting('size');
	
	$self->panic("piece0 was not inwork") if !$so->IsSetAsInwork(0);
	
	if($now_size != $want_size) {
		$self->debug("swapping storage: now_size=$now_size != want_size=$want_size");
		my @old = map({$so->GetSetting($_)} qw(_host _port _url));         # save old settings
		$self->{super}->Queue->RemoveItem($sid);                           # remove old item
		$self->{super}->Admin->ExecuteCommand('history', $sid, 'forget');  # ditch it from history
		$so = $self->_SetupStorage($sid,$want_size,@old);                  # and re-add with correct size
		$self->panic("_SetupStorage failed!") unless $so;                  # shouldn't happen
		$so->SetAsInwork(0);                                               # piece0 was inwork before deletion -> restore status
	}
	
	return $so;
}

##########################################################################
# Registers a new storage item and sets the default settings
sub _SetupStorage {
	my($self,$sid,$size,$host,$port,$url) = @_;
	my $so = $self->{super}->Queue->AddItem(Name=>$sid, Chunks=>1, Overshoot=>0, Size=>$size, Owner=>$self,
	                                        ShaName=>$sid, FileLayout=>[{start=>0, end=>$size, path=>['http_header']}]);
	return undef unless $so;
	$so->SetSetting('type', 'http')    or $self->panic;
	$so->SetSetting('_host',   $host) or $self->panic;
	$so->SetSetting('_port',   $port) or $self->panic;
	$so->SetSetting('_url',    $url)  or $self->panic;
	
	# We've just created a new storage -> stats will be empty so we initialize them right now
	$self->{super}->Queue->InitializeStats($sid);
	$self->{super}->Queue->SetStats($sid, {total_bytes=>$size, total_chunks=>1});
	return $so;
}



sub debug { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }


1;



