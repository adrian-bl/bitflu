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
	$mainclass->Configuration->SetValue('http_maxthreads',                       ($mainclass->Configuration->GetValue('http_maxthreads') || 10) );
	$mainclass->Configuration->SetValue('http_autoloadtorrent', 1) unless defined($mainclass->Configuration->GetValue('http_autoloadtorrent')   );
	
	my $main_socket = $mainclass->Network->NewTcpListen(ID=>$self, Port=>0, MaxPeers=>$mainclass->Configuration->GetValue('http_maxthreads'), DownThrottle=>1,
	                                                    Callbacks=>{Data=>'_Network_Data', Close=>'_Network_Close'});
	$mainclass->AddRunner($self);
	return $self;
}

##########################################################################
# Regsiter admin commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('load', $self, '_StartHttpDownload', "Start download of HTTP-URL",
	  [ [undef, "Bitflu can load files via HTTP (like wget)"], [undef, "To start a http download use: 'load http://www.example.com/foo/bar.tgz'"] ] );
	return 1;
}


sub _StartHttpDownload {
	my($self, @args) = @_;
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	
	foreach my $arg (@args) {
		if(my ($xmode,$xhost,$xport,$xurl) = $arg =~ /^(http|internal\@[^:]+):\/?\/([^\/:]+):?(\d*)\/(.*)$/i) {
			
			$xport ||= 80;
			
			# Design: Hier machen wir einen storage (64 kb?) für den HTTP header!
			# erst wenn dieser fertig ist, machen wir einen storage fuer den eigentlichen download - somit
			# müssen wir keine komischen in-flight events haben.
			
			my $sha = $self->{super}->Tools->sha1_hex("http://$xhost:$xport/$xurl");
			
			### FIXME: WE NEED TO HANDLE INTERNAL LINKS (RSS)
			
			if($self->{super}->Storage->OpenStorage($sha)) {
				push(@MSG, [2, "$sha: Download exists in queue ($arg)"]);
			}
			else {
				my $so = $self->{super}->Queue->AddItem(Name=>$sha, Chunks=>1, Overshoot=>0, Size=>STORAGE_SIZE_DUMMY, Owner=>$self,
				                                        ShaName=>$sha, FileLayout=>[{start=>0, end=>STORAGE_SIZE_DUMMY, path=>['http_header']}]);
				$self->panic("->AddItem failed!") unless $so;
				$so->SetSetting('type', 'http')    or $self->panic;
				$so->SetSetting('_host',   $xhost) or $self->panic;
				$so->SetSetting('_port',   $xport) or $self->panic;
				$so->SetSetting('_url',    $xurl)  or $self->panic;
				$self->resume_this($sha);
				push(@MSG, [1, "$sha: http download queued"]);
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

sub resume_this {
	my($self,$sha) = @_;
	my $so = $self->{super}->Storage->OpenStorage($sha) or $self->panic("could not open $sha !");
	$so->SetAsInwork(0) if $so->IsSetAsFree(0);
	
	# fixme: this is fake
	$self->{super}->Queue->SetStats($sha, {total_bytes=>STORAGE_SIZE_DUMMY, done_bytes=>0, uploaded_bytes=>0, active_clients=>0, clients=>0, speed_upload =>0, speed_download => 0, last_recv => 0,
	                                       total_chunks=>1, done_chunks=>0});
	
	if($so->IsSetAsInwork(0)) {
		$self->_InitiateHttpConnection($sha);
	}
	else {
		$self->warn("$sha : http download finished, nothing to do for us");
	}
	
	# Resume sollte nun einen HTTP request senden:
	# -> header=1 oder nicht komplett -> request senden // else: nichts machen
	# wenn wir den header bekommen haben, koennen wir einfach den storage gegen das richtige austauschen
	# sollten wir KEINEN header bekommen, machen wir einen neuen (5MB?) storage und kopieren die daten AM ENDE in einen richtig gesizten storage
}



sub _InitiateHttpConnection {
	my($self,$sha) = @_;
	my $so       = $self->{super}->Storage->OpenStorage($sha) or $self->panic;
	my $new_sock = $self->{super}->Network->NewTcpConnection(ID=>$self, Port=>$so->GetSetting('_port'),
	                                                         Hostname=>$so->GetSetting('_host'), Timeout=>ESTABLISH_TIMEOUT);
	$self->warn("Shall open http connection for $sha to ".$so->GetSetting('_port'));
	$self->{super}->Network->WriteDataNow($new_sock,"GET / HTTP/1.0\r\n\r\n");
	
	$self->{sockmap}->{$new_sock} = { sha=>$sha, sock=>$new_sock, status=>HEADER_SENT, piggyback=>'' };
	
}

sub _Network_Data {
	my($self, $socket, $bref) = @_;
	my $sm = $self->{sockmap}->{$socket} or $self->panic("No sockmap info for $socket !");
	
	if($sm->{status} == HEADER_SENT && length($sm->{piggyback}) < HEADER_SIZE_MAX) {
		$sm->{piggyback} .= $$bref;
		
		my $hbytes = 0; # size of header
		my $clen   = 0; # content length
		foreach my $line (split(/\r\n/,$sm->{piggyback})) {
			$hbytes += 2+length($line); # line + \r\n
			if(length($line) == 0) {
				# header finished: ditch header from piggyback and move remaining data to new fake buffref
				my $x         = substr($sm->{piggyback},$hbytes);
				$bref         = \$x;
				$sm->{status} = READ_BODY;
				
				$self->warn("HEADER FINISHED: CLEN IS $clen");
				$self->_FixupStorage($sm->{sha}, $clen);
			}
			elsif($line =~ /^Content-Length: (\d+)$/) {
				$clen = $1;
			}
		}
		
	}
	
	# must be after HEADER_SENT if()
	if($sm->{status} == READ_BODY) {
		$self->warn("Got data");
	}
	
}

sub _Network_Close {
	my($self,$socket) = @_;
	$self->warn("CLOSE! $socket");
	delete($self->{sockmap}->{$socket}) or $self->panic("Could not remove $socket from sockmap: did not exist!");
}


##########################################################################
# 
sub _FixupStorage {
	my($self,$sha,$want_clen) = @_;
	
	my $do_clen = ($want_clen || 1024*1024*5);
	
	$self->warn("Should prepare $sha to keep $want_clen // $do_clen bytes");
	
	
	my $so       = $self->{super}->Storage->OpenStorage($sha) or $self->panic("Could not open storage $sha");
	my $now_size = $so->GetSetting('size');
	$self->warn("Storage is currently $now_size bytes big");
}



sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }


1;




=head

use strict;
use constant _BITFLU_APIVERSION => 20110508;
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
	
	my $main_socket = $mainclass->Network->NewTcpListen(ID=>$self, Port=>0, MaxPeers=>$self->{http_maxthreads}, DownThrottle=>1,
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
		$self->warn("$xsha : Still connecting to remote server...");
		$xactive++;
	}
	foreach my $xsk (keys(%{$self->{dlx}->{has_socket}})) {
		if($self->{dlx}->{has_socket}->{$xsk}->{Hash} eq $xsha) {
			$self->warn("$xsha : Still reading HTTP Header from remote....");
			$xactive++;
		}
	}
	if($xactive == 0) {
		$self->{dlx}->{get_socket}->{$xsha} = { Host => $args{Host}, Port => $args{Port}, Url=> $args{Url}, LastRead => $self->{super}->Network->GetTime, Xfails => 0,
		                                        Mode => $args{Mode},  Range => 0, Offset => int($args{Offset}), Hash => $xsha , Name => $xname,
		                                        GotHeader => 0, Length=>0, BsCount=>0, Piggy=>''};
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
		$self->debug("Creating new storage for $args{Hash} ($args{Size})");
		my @pathref = split('/',$args{Host}."/".$args{Name});
		my $name    = $pathref[-1];
		$so = $self->{super}->Queue->AddItem(Name=>$name, Chunks => 1, Overshoot => 0, Size => $args{Size}, Owner => $self,
		                                     ShaName => $args{Hash}, FileLayout => [{ start => 0, end => $args{Size}, path=>\@pathref }]);
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




##########################################################################
# Main loop
sub run {
	my($self,$NOW) = @_;
	
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
	return (int(keys(%{$self->{dlx}->{has_socket}})) ? 0 : 2);
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
	
	$dlx->{Piggy} .= $$buffref;
	$$buffref     = '';

	if($dlx->{GotHeader} == 0) {
		my $bseen     = 0;
		foreach my $line (split(/\r\n/,$dlx->{Piggy})) {
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
				$dlx->{Piggy} = substr($dlx->{Piggy},$bseen);
				$dlx->{GotHeader} = 1;
				unless($dlx->{Storage}) {
					my $this_so = $self->SetupStorage(Name=>$dlx->{Name}, Size=>$dlx->{Length}, Hash=>$dlx->{Hash},Host=>$dlx->{Host}, Port=>$dlx->{Port}, Url=>$dlx->{Url});
					
					unless($dlx->{Storage} = $this_so) {
						# Failed to create the storage
						$self->{super}->Admin->SendNotify($@);
						$self->_KillClient($socket);
						return;
					}
					elsif($dlx->{Mode} =~ /^internal\@/) {
						# internal links got some special kind of storage:
						$self->{super}->Admin->ExecuteCommand('autocommit', $dlx->{Hash}, 'off');
						$self->{super}->Admin->ExecuteCommand('autocancel', $dlx->{Hash}, 'off');
						$self->{super}->Admin->ExecuteCommand('rename',     $dlx->{Hash}, $dlx->{Mode});
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
		my $dlen  = length($dlx->{Piggy});
		my $ddone = $self->{super}->Queue->GetStats($dlx->{Hash})->{done_bytes};
		my $tdone = $dlen + $ddone;
		my $bleft = $self->{super}->Queue->GetStats($dlx->{Hash})->{total_bytes} - $tdone;
		
		if($bleft < 0) {
			$self->warn("$dlx->{Hash}: $dlx->{Host} sent too much data! ($bleft) ; Closing connection with server!");
			$self->_KillClient($socket);
			return undef;
		}
		
		$dlx->{Storage}->WriteData(Chunk => 0, Offset => $ddone, Length => $dlen, Data => \$dlx->{Piggy});
		$self->{super}->Queue->SetStats($dlx->{Hash}, {done_bytes => $tdone});
		
		delete($dlx->{Piggy});
		$dlx->{LastRead} = $self->{super}->Network->GetTime;
		$dlx->{BsCount}  += $dlen if $dlx->{BsCount};
		
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
	
	foreach my $qk (keys(%{$xql->{''.STORAGE_TYPE}}))      { $full_q->{$qk}++       }  # Get all queued HTTP downloads
	foreach my $qk (keys(%{$self->{dlx}->{get_socket}}))   { delete($full_q->{$qk}) }  # Delete all non-downloading (get_socket)
	foreach my $qk (keys(%{$self->{dlx}->{has_socket}})) {
		
		my $dlx = $self->{dlx}->{has_socket}->{$qk};
		
		# Update download statistics:
		$self->{super}->Queue->SetStats($dlx->{Hash}, { speed_download=>int(($dlx->{BsCount})/PICKUP_DELAY) } ) if $dlx->{BsCount};
		$dlx->{BsCount} = 1; # 0 disabled bscount (needed for the first run but DownloadHTTP is a hack so it doesn't matter :-p )
		
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
		my($xsha,$xactive) = $self->_InitDownload(Host=>$xso->GetSetting('_host'), Port=>$xso->GetSetting('_port'), Mode=>STORAGE_TYPE,
		                                          Url=>$xso->GetSetting('_url'), Offset=>$self->{super}->Queue->GetStats($qk)->{done_bytes});
		$self->panic("$xsha != $qk : Unable to resume download $qk ; Recalculate sha1 sum differs") if $xsha ne $qk;
		$self->panic("$qk should be inactive but isn't")                                            if $xactive != 0;
	}
}


sub _Network_Close {
	my($self,$socket) = @_;
	if( (my $dlx = delete($self->{dlx}->{has_socket}->{$socket}) )) {
		$self->{super}->Queue->SetStats($dlx->{Hash}, {active_clients => 0, clients => 0, speed_download=>0});
	}
	$self->debug("CLOSED $socket");
}







1;
=cut
