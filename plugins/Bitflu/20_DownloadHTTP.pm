package Bitflu::DownloadHTTP;
#
# This file is part of 'Bitflu' - (C) 2006-2008 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#
# Fixme: This is just a quick hack and needs a rewrite
#


use strict;
use constant _BITFLU_APIVERSION => 20080611;
use constant HEADER_SIZE_MAX    => 64*1024;   # Size limit for http-headers (64kib should be enough for everyone ;-) )
use constant PICKUP_DELAY       => 30;        # How often shall we scan the queue for 'lost' downloads
use constant TIMEOUT_DELAY      => 60;        # Re-Connect to server if we did not read data within X seconds
use constant STORAGE_TYPE       => 'http';    # Storage Type identifier, do not change.
use constant ESTABLISH_MAXFAILS => 10;        # Drop download if we still could not get a socket after X attemps
use constant ESTABLISH_TIMEOUT  => 5;         # How long to wait for a connection to get established

use constant AUTOTORRENT_MINSIZE => 50;         # Min size of a file to be considered a torrent
use constant AUTOTORRENT_MAXSIZE => 1024*4096;  # Max size of a file to be considered as a torrent

##########################################################################
# Registers the HTTP Plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, nextpickup => 0, nextrun => 0, dlx => { get_socket => {}, has_socket => {} } };
	bless($self,$class);
	
	$self->{http_maxthreads} = ($mainclass->Configuration->GetValue('http_maxthreads') || 10);
	$mainclass->Configuration->SetValue('http_maxthreads', $self->{http_maxthreads});
	
	my $autotorrent = $mainclass->Configuration->GetValue('http_autoloadtorrent');
	$mainclass->Configuration->SetValue('http_autoloadtorrent', 1) unless defined($autotorrent);
	
	my $main_socket = $mainclass->Network->NewTcpListen(ID=>$self, Port=>0, MaxPeers=>$self->{http_maxthreads}, Bind=>$mainclass->Configuration->GetValue('default_bind'),
	                                                    Callbacks =>  {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	$mainclass->AddRunner($self);
	return $self;
}

##########################################################################
# Regsiter admin commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('load', $self, 'StartHTTPDownload', "Start download of HTTP-URL",
	  [ [undef, "Bitflu can load files via HTTP (like wget)"], [undef, "To start a http download use: 'load http://www.example.com/foo/bar.tgz'"] ] );
	return 1;
}

##########################################################################
# Restarts an existing download
sub resume_this {
	my($self, $sid) = @_;
	my $so = $self->{super}->Storage->OpenStorage($sid) or $self->panic("Unable to open/resume $sid");
	$self->SetupStorage(Hash=>$sid); # Request to initialize existing storage / Add the item to queuemgr
}

##########################################################################
# Fire up download
sub StartHTTPDownload {
	my($self, @args) = @_;
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	
	foreach my $arg (@args) {
		if(my ($xhost,$xport,$xurl) = $arg =~ /^http:\/\/([^\/:]+):?(\d*)\/(.+)$/i) {
			$xport ||= 80;
			$xhost = lc($xhost);
			$xurl  = $self->{super}->Tools->UriEscape($self->{super}->Tools->UriUnescape($xurl));
			
			my $xuri = "http://$xhost:$xport/$xurl";
			my ($xsha,$xactive) = $self->_InitDownload(Host=>$xhost, Port=>$xport, Url=>$xurl, Offset=>0);
			
			if($xactive != 0) {
				push(@MSG, [2, "$xsha : Download exists in queue and is still active"]);
			}
			elsif($self->{super}->Storage->OpenStorage($xsha)) {
				push(@MSG, [2, "$xsha: HTTP download exists in queue"]);
				delete($self->{dlx}->{get_socket}->{$xsha}) or $self->panic("Unable to remove get_socket for $xsha !");
			}
			else {
				push(@MSG, [1, "$xsha: HTTP download started"]);
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

##########################################################################
# Create new HTTP-Superfunk object ; kicking the HTTP-Requester
sub _InitDownload {
	my($self, %args) = @_;
	my $xsha = $self->{super}->Tools->sha1_hex("http://$args{Host}:$args{Port}/$args{Url}");
	
	my ($xname) = $args{Url};# =~ /([^\/]+)$/;
	
	my $xactive = 0;
	
	# Check if request to start this download was sent before:
	if ( defined($self->{dlx}->{get_socket}->{$xsha}) ) {
		$self->warn("$xsha : Still getting socket...");
		$xactive++;
	}
	foreach my $xsk (keys(%{$self->{dlx}->{has_socket}})) {
		if($self->{dlx}->{has_socket}->{$xsk}->{Hash} eq $xsha) {
			$self->warn("$xsha : Still reading header...");
			$xactive++;
		}
	}
	
	if($xactive == 0) {
		$self->{dlx}->{get_socket}->{$xsha} = { Host => $args{Host}, Port => $args{Port}, Url=> $args{Url}, LastRead => $self->{super}->Network->GetTime, Xfails => 0,
		                                        Range => 0, Offset => int($args{Offset}), Hash => $xsha , Name => $xname, GotHeader => 0};
	}
	return ($xsha,$xactive);
}


##########################################################################
# Creates a new storage
sub SetupStorage {
	my($self, %args) = @_;
	
	my $so = undef;
	
	my $stats_size = -1;
	my $stats_done = -1;
	
	if($so = $self->{super}->Storage->OpenStorage($args{Hash})) {
		$self->debug("Opened existing storage for $args{Hash}");
		if($so->IsSetAsFree(0))    { $so->SetAsInwork(0) }
		$stats_done = ($so->IsSetAsInwork(0) ? $so->GetSizeOfInworkPiece(0) : $so->GetSizeOfDonePiece(0) );
		$stats_size = $so->GetSetting('size');
	}
	else {
		$self->info("Creating new storage for $args{Hash} ($args{Size})");
		my @pathref = split('/',$args{Host}."/".$args{Name});
		my $name    = $pathref[-1];
		$so = $self->{super}->Queue->AddItem(Name=>$name, Chunks => 1, Overshoot => 0, Size => $args{Size}, Owner => $self,
		                                     ShaName => $args{Hash}, FileLayout => { $args{Name} => { start => 0, end => $args{Size}, path=>\@pathref } });
		return 0 unless $so; # Failed. $@ is set
		
		$so->SetSetting('type', STORAGE_TYPE) or $self->panic;
		$so->SetSetting('_host', $args{Host}) or $self->panic;
		$so->SetSetting('_port', $args{Port}) or $self->panic;
		$so->SetSetting('_url',  $args{Url})  or $self->panic;
		$stats_size = $args{Size};
		$stats_done = 0;
		$so->SetAsInwork(0);
		$so->Truncate(0); # Do not do funny things
	}
	
	$self->{super}->Queue->SetStats($args{Hash}, {total_bytes=>$stats_size, done_bytes=>$stats_done, uploaded_bytes=>($so->GetSetting('_uploaded_bytes') || 0),
	                                              active_clients=>0, clients=>0, speed_upload =>0, speed_download => 0,
	                                              last_recv => 0, total_chunks=>1, done_chunks=>($so->IsSetAsDone(0) ? 1 : 0 )});
	return $so;
}





sub run {
	my($self) = @_;
	
	$self->{super}->Network->Run($self);
	my $NOW = $self->{super}->Network->GetTime;
	
	if( $NOW > $self->{nextpickup} ) {
		$self->_Pickup;
	}
	
	if( $NOW > $self->{nextrun} ) {
	$self->{nextrun} = $NOW+(ESTABLISH_TIMEOUT);
	foreach my $nsock (keys(%{$self->{dlx}->{get_socket}})) {
			# Establish new TCP-Connections
			my $new_sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$self->{dlx}->{get_socket}->{$nsock}->{Port},
			                                                         Hostname=>$self->{dlx}->{get_socket}->{$nsock}->{Host}, Timeout=>ESTABLISH_TIMEOUT);
			if(defined($new_sock)) {
				my $wdata  = "GET /$self->{dlx}->{get_socket}->{$nsock}->{Url} HTTP/1.1\r\n";
				   $wdata .= "Host: $self->{dlx}->{get_socket}->{$nsock}->{Host}\r\n";
				   $wdata .= "Range: bytes=".int($self->{dlx}->{get_socket}->{$nsock}->{Offset})."-\r\n";
				   $wdata .= "Connection: Close\r\n\r\n";
				$self->{super}->Network->WriteData($new_sock, $wdata) or $self->panic("Unable to write data to $new_sock !");
				$self->{dlx}->{has_socket}->{$new_sock} = delete($self->{dlx}->{get_socket}->{$nsock});
				$self->{dlx}->{has_socket}->{$new_sock}->{Socket} = $new_sock;
			}
			elsif(++$self->{dlx}->{get_socket}->{$nsock}->{Xfails} > ESTABLISH_MAXFAILS) {
				$self->{super}->Admin->SendNotify("Unable to connect to ".$self->{dlx}->{get_socket}->{$nsock}->{Host}." , dropping download");
				delete($self->{dlx}->{get_socket}->{$nsock}) or $self->panic("Unable to delete existing download");
			}
		}
	}
}

sub cancel_this {
	my($self,$sid) = @_;
	unless(delete($self->{dlx}->{get_socket}->{$sid})) {
		# -> Full search, nah!
		foreach my $qk (keys(%{$self->{dlx}->{has_socket}})) {
			if($self->{dlx}->{has_socket}->{$qk}->{Hash} eq $sid) {
				delete($self->{dlx}->{has_socket}->{$qk}) or $self->panic("Unable to delete $qk");
			}
		}
	}
	$self->{super}->Queue->RemoveItem($sid);
}

sub _Network_Data {
	my($self, $socket, $buffref) = @_;
	
	my $dlx = $self->{dlx}->{has_socket}->{$socket};
	unless(defined($dlx)) {
		$self->warn("Closing connection with $socket ; stale handle");
		$self->_KillClient($socket);
		return;
	}
	
	$dlx->{piggy} .= $$buffref;
	$$buffref     = '';

	if($dlx->{GotHeader} == 0) {
		my $bseen     = 0;
		foreach my $line (split(/\r\n/,$dlx->{piggy})) {
			$bseen += (2+length($line));
			
			if($line =~ /^Content-Length: (\d+)$/) {
				$dlx->{Length} = $1;
			}
			elsif($line =~ /^Content-Range: bytes (\d+)-/) {
				$dlx->{Range}  = $1;
			}
			
			if($bseen >= HEADER_SIZE_MAX) {
				$self->{super}->Admin->SendNotify("$dlx->{Hash}: HTTP-Header received from '$dlx->{Host}' is waaay too big ($bseen bytes) ; Dropping connection.");
				$self->_KillClient($socket);
				return;
			}
			elsif(length($line) == 0 && $dlx->{Length} == 0) {
				$self->{super}->Admin->SendNotify("$dlx->{Hash}: '$dlx->{Host}' did not specify size of download ; Dropping connection.");
				$self->_KillClient($socket);
				return;
			}
			elsif(length($line) == 0) {
				$dlx->{piggy} = substr($dlx->{piggy},$bseen);
				$dlx->{GotHeader} = 1;
				unless($dlx->{Storage}) {
					my $this_so = $self->SetupStorage(Name=>$dlx->{Name}, Size=>$dlx->{Length}, Hash=>$dlx->{Hash}, Host=>$dlx->{Host}, Port=>$dlx->{Port}, Url=>$dlx->{Url});
					unless($dlx->{Storage} = $this_so) {
						$self->{super}->Admin->SendNotify($@);
						$self->_KillClient($socket);
						return;
					}
				}
				$self->{super}->Queue->SetStats($dlx->{Hash}, {active_clients => 1, clients => 1});
				if($dlx->{Range} != $dlx->{Offset}) {
					$self->{super}->Admin->SendNotify("$dlx->{Hash}: Webserver does not support HTTP-Ranges, re-starting download from scratch :-(");
					$dlx->{Storage}->Truncate(0);
					$self->{super}->Queue->SetStats($dlx->{Hash}, {done_bytes => 0 });
				}
				
				last;
			}
		}
	}
	
	if($dlx->{GotHeader} != 0) {
		my $dlen  = length($dlx->{piggy});
		my $ddone = $self->{super}->Queue->GetStats($dlx->{Hash})->{done_bytes};
		my $tdone = $dlen + $ddone;
		my $bleft = $self->{super}->Queue->GetStats($dlx->{Hash})->{total_bytes} - $tdone;
		
		if($bleft < 0) {
			$self->warn("$dlx->{Hash}: $dlx->{Host} sent too much data! ($bleft) ; Closing connection with server!");
			$self->_KillClient($socket);
			return undef;
		}
		
		$dlx->{Storage}->WriteData(Chunk => 0, Offset => $ddone, Length => $dlen, Data => \$dlx->{piggy});
		$self->{super}->Queue->SetStats($dlx->{Hash}, {done_bytes => $tdone});
		delete($dlx->{piggy});
		$dlx->{LastRead} = $self->{super}->Network->GetTime;
		if($bleft == 0) {
			$dlx->{Storage}->SetAsDone(0);
			$self->{super}->Queue->SetStats($dlx->{Hash}, {done_chunks => 1, uploaded_bytes => $self->{super}->Configuration->GetValue('autocancel')*$tdone }); # Force a drop
			$dlx->{Storage}->SetSetting('_uploaded_bytes', $self->{super}->Queue->GetStats($dlx->{Hash})->{uploaded_bytes});
			
			if($self->{super}->Configuration->GetValue('http_autoloadtorrent')) {
				$self->_AutoMove($dlx);
			}
		}
	}
}


##########################################################################
# Move a http-download (aka chunk 0) into the autoload folder
# if file appears to be a torrent
sub _AutoMove {
	my($self,$dobj) = @_;
	my $total_size = $dobj->{Storage}->GetSizeOfDonePiece(0);
	if($total_size >= AUTOTORRENT_MINSIZE && $total_size <= AUTOTORRENT_MAXSIZE) {
		my $xbuff = $dobj->{Storage}->ReadDoneData(Offset=>0, Length=>$total_size, Chunk=>0);
		if($xbuff =~ /^d\d+\:[^:]+[0-9e]\:/) {
			
			my $notifymsg = $dobj->{Hash}." : ";
			my $destfile  = sprintf("%s/%x-%x-%x.http_download", $self->{super}->Configuration->GetValue('autoload_dir'), $$, int(rand(0xFFFFFF)), int(time()));
			if( open(DEST, ">", $destfile) ) {
				print DEST $xbuff;
				close(DEST);
				$self->{super}->Admin->ExecuteCommand('autoload');
				$self->{super}->Admin->ExecuteCommand('cancel', $dobj->{Hash});
				$notifymsg .= " .torrent file has been moved into your autoload folder";
			}
			else {
				$notifymsg .= "failed to copy .torrent to '$destfile' : $!";
			}
			$self->{super}->Admin->SendNotify($notifymsg);
		}
	}
}




sub _KillClient {
	my($self,$socket) = @_;
	$self->_Network_Close($socket);
	$self->{super}->Network->RemoveSocket($self, $socket);
	return undef;
}

sub _Pickup {
	my($self) = @_;
	my $NOW = $self->{super}->Network->GetTime;
	$self->{nextpickup} = $NOW+PICKUP_DELAY;
	my $full_q = ();
	my $xql    = $self->{super}->Queue->GetQueueList;
	foreach my $qk (keys(%{$xql->{''.STORAGE_TYPE}}))      { $full_q->{$qk}++       }
	foreach my $qk (keys(%{$self->{dlx}->{get_socket}}))   { delete($full_q->{$qk}) }
	foreach my $qk (keys(%{$self->{dlx}->{has_socket}})) {
		my $dlx = $self->{dlx}->{has_socket}->{$qk};
		if(($dlx->{LastRead}+TIMEOUT_DELAY < $self->{super}->Network->GetTime) ) {
			$self->warn("$dlx->{Hash} : Attemping to re-connect");
			$self->_KillClient($dlx->{Socket});
		}
		else {
			delete($full_q->{$dlx->{Hash}}) or $self->warn("$dlx->{Hash} does not exist in queue but is active, ?!");
		}
	}
	
	foreach my $qk (keys(%$full_q)) {
		my $xso = $self->{super}->Storage->OpenStorage($qk) or $self->panic("Unable to resume $qk");
		next unless $xso->IsSetAsInwork(0);
		$self->info("Resuming incomplete download '$qk'");
		my($xsha,$xactive) = $self->_InitDownload(Host=>$xso->GetSetting('_host'), Port=>$xso->GetSetting('_port'),
		                                          Url=>$xso->GetSetting('_url'), Offset=>$self->{super}->Queue->GetStats($qk)->{done_bytes});
		$self->panic("$xsha != $qk : Unable to resume download $qk ; Recalculate sha1 sum differs") if $xsha ne $qk;
		$self->panic("$qk should be inactive but isn't")                                            if $xactive != 0;
	}
}

sub _Network_Close {
	my($self,$socket) = @_;
	if( (my $dlx = delete($self->{dlx}->{has_socket}->{$socket}) )) {
		$self->{super}->Queue->SetStats($dlx->{Hash}, {active_clients => 0, clients => 0});
	}
	$self->debug("CLOSED $socket");
}






sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }


1;
