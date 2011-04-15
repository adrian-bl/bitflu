package Bitflu::AdminHTTP;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2008-2010 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#

use strict;
use POSIX;
use constant _BITFLU_APIVERSION => 20110306;

use constant STATE_READHEADER   => 1;
use constant STATE_SENDBODY     => 2;
use constant SOCKET_TIMEOUT     => 8;
use constant BUFF_MAXONWIRE     => Bitflu::Network::MAXONWIRE;
use constant BUFF_BUFSIZE       => Bitflu::Network::BF_BUFSIZ;
use constant NOTIFY_BUFF        => 10;

use constant CTYPE_BINARY       => 'binary/octet-stream';
use constant CTYPE_HTML         => 'text/html';
use constant CTYPE_PNG          => 'image/png';
use constant CTYPE_GIF          => 'image/gif';
use constant CTYPE_JPG          => 'image/jpeg';
use constant CTYPE_TEXT         => 'text/plain';

##########################################################################
# Register this plugin
sub register {
	my($class,$mainclass) = @_;
	
	my $NOW = $mainclass->Network->GetTime;
	
	my $self = { super => $mainclass, sockets => {}, data_dp => Bitflu::AdminHTTP::Data->new(super=>$mainclass), notify => { end=>$NOW, start=>$NOW, ref => {} } };
	bless($self,$class);
	
	my $xconf = { webgui_bind => '127.0.0.1', webgui_port=>4081 };
	
	foreach my $funk qw(webgui_port webgui_bind) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		if(defined($this_value)) {
			$xconf->{$funk} = $this_value;
		}
		else {
			$mainclass->Configuration->SetValue($funk,$xconf->{$funk});
		}
		$mainclass->Configuration->RuntimeLockValue($funk);
	}
	
	
	
	my $sock = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$xconf->{webgui_port}, Bind=>$xconf->{webgui_bind}, DownThrottle=>0, 
	                                             MaxPeers=>30, Callbacks =>  {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	unless($sock) {
		$self->stop("Unable to bind to $xconf->{webgui_bind}:$xconf->{webgui_port} : $!");
	}
	
	$self->info(" >> Web-GUI ready, visit http://$xconf->{webgui_bind}:$xconf->{webgui_port}");
	
	$mainclass->Admin->RegisterNotify($self, "_Receive_Notify");
	return $self;
}

##########################################################################
# Register  private commands
sub init {
	my($self) = @_;
	return 1;
}

##########################################################################
# This is our SxTask callback
# We check if the socket is still alive: If it isn't: Drop the connection (fixme: race condition? > SXTASK INFRA PROBLEM)
#
sub WatchSocket {
	my($self,$socknam) = @_;
	
	my $sockstat = $self->GetSockState($socknam);          # panics if sock does not exist (SHOULD NEVER HAPPEN because SxTasks are killed on close)
	my $sockref  = $self->{sockets}->{$socknam};
	my $sockglob = $sockref->{socket};
	
	if($sockstat == STATE_SENDBODY) {
		
		my $qfree  = $self->{super}->Network->GetQueueFree($sockglob);
		my $stream = $self->GetStreamJob($sockglob);
		
		if($qfree == 0) {
			# Void: Nothing to send
		}
		elsif(defined($stream->{sid}) && (my $so = $self->{super}->Storage->OpenStorage($stream->{sid}))) {
			# -> Job has a stream -> refill buffer
			my $buff    = undef; # Network buffer
			my $bufflen = -1;    # Length of buffer
			my $lchunk  = -1;    # LastChunk
			for(0..99) {
				
				# Fillup buffer if lchunk doesn't match current stream->{chunk}
				if($stream->{chunk} != $lchunk) {
					($buff,undef) = $so->GetFileChunk($stream->{file}, $stream->{chunk});
					if(defined($buff)) { $lchunk = $stream->{chunk}; $bufflen=length($buff); }
					else               { $self->DropStreamJob($sockglob); last;              }
				}
				
				my $stream_offset  = $stream->{offset};
				my $buff_unwritten = $bufflen-$stream_offset;
				my $can_write      = ( $buff_unwritten > $qfree ? $qfree : $buff_unwritten );
				
				$self->{super}->Network->WriteDataNow($sockglob,substr($buff,$stream_offset,$can_write)) or $self->panic("Write buffer filled up but shouldn't have");
				
				# Do we have an offset or can we go to the next chunk?
				if($buff_unwritten-$can_write > 0) { $stream->{offset} += $can_write;   }
				else                               { $self->AdvanceStreamJob($sockglob);}
				
				# Fixup qfree
				$qfree = $self->{super}->Network->GetQueueFree($sockglob);
				last unless $qfree;
			}
			
		}
		elsif($self->{super}->Network->GetQueueLen($sockglob) == 0) {
			$self->DropConnection($sockglob); # also kills SxTask itself
		}
	}
	elsif($sockstat == STATE_READHEADER) {
		if($sockref->{timeout_at} <= $self->{super}->Network->GetTime) {
			$self->info("Timeout while reading header from $sockglob : closing connection, bye!");
			$self->DropConnection($sockglob);
		}
	}
	else {
		$self->panic("Socket in wrong state! $sockstat");
	}
	
	return 1; # always return TRUE -> We are going to kill SxTasks ourself
}

##########################################################################
# Handles a full HTTP request
sub HandleHttpRequest {
	my($self, %args) = @_;
	
	my $sock   = $args{Socket};
	my $header = $args{Header};
	my $body   = $args{Body};
	my $honly  = $args{HeadersOnly};
	my $sr     = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	my $rq     = $self->BufferToHttpHeader($header);
	
	my $ctype = 'application/jsonrequest';
	my $data  = '';
	
	if(!$self->{super}->Admin->AuthenticateUser(User=>'', Pass=>'')) {
		my $auth_result = 0;
		if(exists($rq->{authorization}) && $rq->{authorization} =~ /^Basic (.+)$/) {
			my($this_user, $this_pass) = split(/:/, $self->{super}->Tools->decode_b64($1),2);
			$auth_result = $self->{super}->Admin->AuthenticateUser(User=>$this_user, Pass=>$this_pass);
		}
		unless($auth_result) {
			$self->HttpSendUnauthorized($sock);
			return;
		}
	}
	
	if($rq->{URI} eq '/torrentList') {
		$data = $self->_JSON_TorrentList;
	}
	elsif($rq->{URI} eq '/stats') {
		$data = $self->_JSON_GlobalStats;
	}
	elsif($rq->{URI} =~ /^\/info\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_InfoTorrent($1);
	}
	elsif($rq->{URI} =~ /^\/cancel\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('cancel', $1);
	}
	elsif($rq->{URI} =~ /^\/wipe\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('cancel', '--wipe', $1);
	}
	elsif($rq->{URI} =~ /^\/pause\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('pause', $1);
	}
	elsif($rq->{URI} =~ /^\/resume\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('resume', $1);
	}
	elsif($rq->{URI} =~ /^\/exclude\/([a-z0-9]{40})\/(\d+)$/) {
		$self->{super}->Admin->ExecuteCommand('files', $1, 'exclude', $2);
	}
	elsif($rq->{URI} =~ /^\/include\/([a-z0-9]{40})\/(\d+)$/) {
		$self->{super}->Admin->ExecuteCommand('files', $1, 'include', $2);
	}
	elsif($rq->{URI} =~ /^\/showfiles\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_ShowFiles($1);
	}
	elsif($rq->{URI} =~ /^\/showfiles-ext\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_ShowFilesExtended($1);
	}
	elsif($rq->{URI} =~ /^\/peerlist\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_ShowPeers($1);
	}
	elsif($rq->{URI} =~ /^\/startdownload\/(.+)$/) {
		my $url = $self->{super}->Tools->UriUnescape($1);
		$data = $self->_JSON_StartDownload($url);
	}
	elsif($rq->{URI} =~ /^\/recvnotify\/(\d+)$/) {
		$data = $self->_JSON_RecvNotify($1);
	}
	elsif($rq->{URI} =~ /^\/history\/(.+)$/) {
		$data = $self->_JSON_HistoryAction($1);
	}
	elsif($rq->{URI} =~ /^\/configuration\/(get|set)\/(.*)$/) {
		$data = $self->_JSON_ConfigurationAction($1,$2);
	}
	elsif($rq->{URI} =~ /^\/createtorrent\/(.+)$/) {
		my $url = $self->{super}->Tools->UriUnescape($1);
		$data = $self->_JSON_CreateTorrentAction($url);
	}
	elsif($rq->{METHOD} eq 'POST' && $rq->{URI} =~ /^\/new_torrent_httpui$/) {
		$data  = $self->_JSON_NewTorrentAction($body);
		$ctype = CTYPE_HTML; # XXX Hack
	}
	elsif($rq->{URI} eq '/browse/') {
		$ctype = CTYPE_HTML;
		$data  = $self->CreateToplevelDirlist;
	}
	elsif( (my($xh,$xrq) = $rq->{URI} =~ /^\/getfile\/([a-z0-9]{40})\/(.+)$/) && (my $so = $self->{super}->Storage->OpenStorage($1)) ) {
		
		my $file_id = 0;
		my $file_ct = CTYPE_BINARY;
		if($xrq =~ /^browse\/(.*)$/) { # Browse link -> Dispatch
			($data,$file_ct,$file_id) = $self->CreateFakeDirlisting(Storage=>$so, Filter=>$1, Hash=>$xh);
		}
		
		if($file_id or ( ($file_id) = $xrq =~ /^(\d+)$/ ) ) {
			$file_id = abs(int($file_id-1)); # 'GUI' starts at 1 / Storage at 0
			if($so->GetFileCount > $file_id) {
				my $finfo  = $so->GetFileInfo($file_id);
				my ($fnam) = $finfo->{path} =~ /([^\/]+)$/;
				$fnam      = $self->_sEsc($fnam);
				
				my $cdisp = 'filename="'.$fnam.'"';                           # Set filename for this download
				   $cdisp = "attachment; $cdisp" if $file_ct eq CTYPE_BINARY; # Add 'attachment' option for binary files
				
				$self->HttpSendOkStream($sock, 'Content-Length'=>$finfo->{size}, 'Content-Disposition' => $cdisp, 'Content-Type'=>$file_ct);
				
				unless($honly) {
					$self->AddStreamJob($sock,$xh,$file_id);
				}
				return; # we sent our own headers
			}
		}
		
		if(length($data)) {
			$ctype = CTYPE_HTML; # -> Dirlist
		}
		else {
			$self->HttpSendNotFound($sock);
			return; # abort , do not send OK headers
		}
		
	}
	else {
		($ctype, $data) = $self->Data->Get($rq->{URI});
	}
	
	$self->HttpSendOk($sock, Payload => $data, 'Content-Type' => $ctype, HeadersOnly=>$honly);
}


##########################################################################
# Receive a tion (Called via Admin)
sub _Receive_Notify {
	my($self, $string) = @_;
	
	my $nx                = $self->{notify};
	my $ditch_id          = $nx->{end}-(NOTIFY_BUFF);
	my $new_id            = $nx->{end}++;
	$nx->{ref}->{$new_id} = $string;
	
	if(delete($nx->{ref}->{$ditch_id})) {
		$nx->{start} = $ditch_id+1;
	}
}




##########################################################################
# Read data from network
sub _Network_Data {
	my($self,$sock,$buffref, $len) = @_;
	my $state = $self->GetSockState($sock);
	
	if($state == STATE_READHEADER) {
		$self->AddSockBuff($sock,$buffref,$len);
		my ($buff,$bufflen) = $self->GetSockBuff($sock);
		my ($header, $body) = $self->_SplitHttpRequest($buff);
		return unless defined $body;
		
		my ($req) = split(qr/\r?\n/, $header);

		if($req =~ m/^(GET|HEAD) /) {
			$self->HandleHttpRequest(Socket=>$sock, Header=>$header, Body=>$body, HeadersOnly=>($1 eq 'HEAD' ? 1 : 0) );
		}
		elsif ($req =~ m/^POST /) {
			my ($expected) = $header =~ /^Content-Length:\s*(\d+)/mi;
			my ($boundary) = $header =~ /^Content-Type:.+boundary=([^\r\n; ]+)/mi;
			if (!defined $expected) {
				$self->warn('Bad request, POST missing Content-Length');
				$self->HttpSendBadRequest($sock);
				return;
			}
			return if $expected > length($body); # Request not fully sent
			
			if(defined($boundary)) {
				# -> Upload had a boundary: remove it
				my $off = 0;
				foreach(split(/\n/, $body)) {
					$off += length($_)+1;
					last if length($_) == 1;
				}
				$body = substr($body, $off, -1*( length($boundary)+8 ) );
			}
			
			
			$self->HandleHttpRequest(Socket=>$sock, Header=>$header, Body=>$body, HeadersOnly=>1);
		}
		else {
			$self->warn('Method not supported: ' . substr($header, 0, 10) . '...');
			$self->HttpSendMethodNotAllowed($sock);
		}
	}
	else {
		$self->warn("<$sock>: Ignoring data in state $state");
	}
}

##########################################################################
# Split a buffer with a HTTP request into headers and body
sub _SplitHttpRequest {
	my ($self, $buff) = @_;
	return split(qr/\r\n\r\n/, $buff, 2);
}

##########################################################################
# Accept new incoming connection
sub _Network_Accept {
	my($self,$sock) = @_;
	$self->AddSocket($sock);
	
}

##########################################################################
# Close down TCP connection
sub _Network_Close {
	my($self,$sock) =  @_;
	$self->RemoveSocket($sock);
}




##########################################################################
# Register new socket
sub AddSocket {
	my($self, $sock) = @_;
	$self->panic("Duplicate socket <$sock>!") if exists($self->{sockets}->{$sock});
	$self->{sockets}->{$sock} = { buffer => '', bufflen => 0, socket=>$sock, state=>STATE_READHEADER, stream => {},
	                              sxtask=>undef, timeout_at => $self->{super}->Network->GetTime+SOCKET_TIMEOUT };
	$self->DropStreamJob($sock); # inits stream-ref
	$self->{sockets}->{$sock}->{sxtask} = $self->{super}->CreateSxTask(Args=>[$sock], Interval=>0, Superclass=>$self, Callback=>'WatchSocket');
}

##########################################################################
# Remove a socket
sub RemoveSocket {
	my($self, $sock) = @_;
	my $sref = delete($self->{sockets}->{$sock}) or $self->panic("Unable to remove <$sock>, did not exist?!");
	$sref->{sxtask}->destroy # Destroy sxtask (all socks have an sxtask : it's added by AddSocket)
}

##########################################################################
# Register Streaming-Job
sub AddStreamJob {
	my($self,$sock,$sid,$file) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	$sr->{stream} = { sid=>$sid, file=>$file, chunk=>0, offset=>0 };
}

##########################################################################
# Returns a stream-job
sub GetStreamJob {
	my($self,$sock) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	return $sr->{stream};
}

##########################################################################
sub AdvanceStreamJob {
	my($self,$sock) = @_;
	my $sj = $self->GetStreamJob($sock);
	$sj->{offset} = 0;
	$sj->{chunk}++;
}

##########################################################################
sub DropStreamJob {
	my($self,$sock) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	$sr->{stream} = { sid=>undef, file=>-1, chunk=>-1 };
}

##########################################################################
# Add data to socket buffer
sub AddSockBuff {
	my($self, $sock, $buffref, $len) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	
	if($sr->{bufflen} <= BUFF_MAXONWIRE) {
		# Note: This doesn't limit bufflen exactly but it's good enough:
		#       A single AddSockBuff call will never get more than a few
		#       POSIX::BUFSIZ as payload.
		$sr->{buffer}  .= ${$buffref};
		$sr->{bufflen} += $len;
	}
	else {
		$self->warn("Buffer for <$sock> is full, ignoring new data");
	}
}

##########################################################################
# Returns an array of all connected sockets
sub GetSockets {
	my($self, $sock) = @_;
	return keys(%{$self->{sockets}});
}

##########################################################################
# Returns true if sock is still connected
sub ExistsSock {
	my($self,$sock) = @_;
	return (exists($self->{sockets}->{$sock}) ? 1 : 0 );
}

##########################################################################
# Returns buffered data from socket
sub GetSockBuff {
	my($self,$sock) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	return ($sr->{buffer}, $sr->{bufflen});
}

##########################################################################
# Returns status of a socket
sub GetSockState {
	my($self,$sock) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	return $sr->{state};
}

##########################################################################
# Sets the status
sub SetSockState {
	my($self,$sock,$state) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	return $sr->{state} = $state;
}


##########################################################################
# Parse current buffer and return http-reference
sub BufferToHttpHeader {
	my($self,$header) = @_;
	my $ref   = {URI=>'/', METHOD=>'GET'};
	
	my @lines = split(/\r\n/,$header);
	my $rq    = shift(@lines);
	
	foreach my $tag (@lines) {
		if($tag =~ /^([^:]+): (.*)$/) {
			$ref->{lc($1)} = $2;
		}
	}
	if($rq =~ /^(GET|HEAD|POST) \/*(\/\S*)/) {
		$ref->{METHOD} = uc($1);
		$ref->{URI}    = $2;
	}
	return $ref;
}


##########################################################################
# Overview with all downloads
# this is in /browse/, so we should use ../ if we are behind a proxy
sub CreateToplevelDirlist {
	my($self) = @_;
	my $qlist = $self->{super}->Queue->GetQueueList;
	
	my $buff  = qq{<html><head><meta http-equiv="content-type" content="text/html; charset=utf-8">
		<title>Bitflu Downloads</title></head><body background="../bg_white.png"><h2 style="background:url(../bg_lblue.png);">
		Download overview</h2><table border=0>};
	
	foreach my $dl_type (sort(keys(%$qlist))) {
		foreach my $key (sort(keys(%{$qlist->{$dl_type}}))) {
			my $dlso   = $self->{super}->Storage->OpenStorage($key) or $self->panic;
			my $stats  = $self->{super}->Queue->GetStats($key)      or $self->panic;
			my $dlname = $self->_hEsc($dlso->GetSetting('name'));
			my $done   = ($stats->{done_chunks}/$stats->{total_chunks})*100;
			   $done   = 99.9 if $done == 100 && $stats->{done_chunks} != $stats->{total_chunks}; # round
			   $done   = sprintf("%5.1f%%",$done);

			$buff .= "<tr><td align=right>$done</td><td>&nbsp;</td><td><img src='../ic_dir.png'></td><td><a href='../getfile/$key/browse/'>$dlname</a></td></tr>\n";
		}
	}
	$buff .= "</table><br /><br /><div align=right><i>Generated by Bitflu/".$self->{super}->GetVersionString." at ".gmtime()." (GMT)</i></div> </body></html>";
	return $buff;
}

##########################################################################
# Creates an apache-like dirlisting for given path
sub CreateFakeDirlisting {
	my($self, %args) = @_;
	my $so     = $args{Storage};
	my $stats  = $self->{super}->Queue->GetStats($args{Hash});
	my $filter = "/".$self->{super}->Tools->UriUnescape($args{Filter});
	my $fslash = $filter =~ tr/\///;
	my $desc   = $self->_hEsc($so->GetSetting('name').$filter);
	my $dlist  = ( $filter =~ /\/$/ ? 1 : 0 );        # Are we in dirlist mode?
	my $rb     = join('', map('../', (-1..$fslash))); # calculcate relative backlink from filter
	
	# Build dirlist.
	# Filter ALWAYS starts with a '/' and ends with / if we are in dirlist mode
	
	my $dl_dirs  = {};
	my $dl_files = {};
	for(my $i=0; $i < $so->GetFileCount; $i++) {
		my $this_file = $so->GetFileInfo($i);
		my $path      = "/".$this_file->{path};
		if( $dlist && (my ($rest) = $path =~ /^\Q$filter\E(.+)/) ) {
			if($rest =~ /^([^\/]+)\//) { $dl_dirs->{$1}++;       }
			else                       { $dl_files->{$rest} = $i;}
		}
		elsif($filter eq $path) {
			return('',$self->GuessContentType($path),$i+1); # cannot be 0 -> consistency with GUI
		}
	}
	
	# nothing found?
	if( int(keys(%$dl_dirs))+int(keys(%$dl_files)) == 0 ) {
		return('invalid directory', '',0);
	}
	
	my $total_done = ($stats->{done_chunks}/$stats->{total_chunks})*100;
	   $total_done = 99.9 if $total_done == 100 && $stats->{done_chunks} != $stats->{total_chunks}; # round
	   $total_done = sprintf("%5.1f%%",$total_done);
	
	my $buff = qq{<html><head><meta http-equiv="content-type" content="text/html; charset=utf-8">
		<title>Index of $desc</title></head><body background="${rb}bg_white.png"><h2 style="background:url(${rb}bg_lblue.png);">
		[$total_done] Index of: $desc</h2><table border=0 width=100%>};
	
	$buff .= "<tr><td width=14></td><td></td><td width=120></td></tr>\n";
	
	if($filter eq '/') { # not at toplevel dir -> add backlink (we are at: getfile/hash/browse/
		$buff .= "<tr><td><img src=${rb}ic_back.png></td><td><a href='../../../browse/'>&nbsp;Overview</a></td><td></td></tr>\n";
	}
	else {
		$buff .= "<tr><td><img src=${rb}ic_back.png></td><td><a href='../'>&nbsp;Back</a></td><td></td></tr>\n";
	}
	
	$buff .= "<tr><td></td><td></td><td></td></tr>\n";
	
	foreach my $dirname (sort(keys(%$dl_dirs))) {
		my $si  = $dl_dirs->{$dirname};
		my $txt = "subitem".($si==1?'':'s');
		$buff .= "<tr><td><img src=${rb}ic_dir.png></td><td><a href=\"".$self->{super}->Tools->UriEscape($dirname)."/\">";
		$buff .= $self->_hEsc($dirname)."</a></td><td>[$si $txt]</td></tr>\n";
	}
	
	foreach my $filename (sort(keys(%$dl_files))) {
		my $fpr    = $so->GetFileProgress($dl_files->{$filename});
		my $done   = $fpr->{done};
		my $chunks = $fpr->{chunks};
		my $percent= ($chunks > 0 ? ($done/$chunks*100) : 100);
		my $pcolor = ($done==$chunks?'green':'red');
		my $phtml  = "<div style='float:right;height:10px;width:50px;border: 1px solid black;'><div style='height:10px;width:".int($percent/2)."px;background:$pcolor'></div></div>";
		
		my $fsize  = sprintf("%5.1fM", ($chunks*$fpr->{chunksize}/1024/1024));
		   $fsize  = "~$fsize" if $chunks == 1;
		$buff .= "<tr title='$percent%'><td><img src=${rb}ic_file.png></td><td>";
		$buff .= "<a href=\"".$self->{super}->Tools->UriEscape($filename)."\">".$self->_hEsc($filename)."</a>";
		$buff .= "</td><td><div>$fsize&nbsp;$phtml</div></td></tr>\n";
	}
	
	$buff .= "</table><br /><br /><hr> <div align=right><i>Generated by Bitflu/".$self->{super}->GetVersionString." at ".gmtime()." (GMT)</i></div> </body></html>";
	return ($buff,'',0);
}

##########################################################################
# return content type based on file extension
sub GuessContentType {
	my($self,$file) = @_;
	return CTYPE_HTML  if $file =~ /\.(htm|html)$/i;
	return CTYPE_PNG   if $file =~ /\.(png)$/i;
	return CTYPE_GIF   if $file =~ /\.(gif)$/i;
	return CTYPE_JPG   if $file =~ /\.(jpg|jpeg)$/i;
	return CTYPE_TEXT  if $file =~ /\.(txt|sfv|nfo|asc|md5)$/i;
	return CTYPE_BINARY;
}


##########################################################################
# Drop connection from our site
sub DropConnection {
	my($self,$sock) = @_;
	$self->_Network_Close($sock);
	$self->{super}->Network->RemoveSocket($self,$sock);
}

sub Data {
	my($self) = @_;
	return $self->{data_dp};
}

sub debug { my($self, $msg) = @_; $self->{super}->debug("WebGUI  : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("WebGUI  : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("WebGUI  : ".$msg); }
sub warn  { my($self, $msg) = @_; $self->{super}->warn("WebGUI  : ".$msg); }
sub stop { my($self, $msg) = @_;  $self->{super}->stop("WebGUI  : ".$msg); }


####################################################################################################################################################
####################################################################################################################################################
# HTTP Header Stuff


sub HttpSendOk {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>200, 'Content-Length'=>length($args{Payload}), 'Content-Type'=>$args{'Content-Type'});
	$self->{super}->Network->WriteDataNow($sock, $args{Payload}) unless $args{HeadersOnly};
}

sub HttpSendOkStream {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>200, 'Content-Length'=>$args{'Content-Length'},
	                              'Content-Disposition' => $args{'Content-Disposition'}, 'Content-Type'=>$args{'Content-Type'});
}

sub HttpSendBadRequest {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>400);
}

sub HttpSendNotFound {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>404);
}

sub HttpSendMethodNotAllowed {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>405);
}

sub HttpSendUnauthorized {
	my($self, $sock) = @_;
	$self->_HttpSendHeader($sock, Scode=>401, 'WWW-Authenticate' => 'Basic realm="Bitflu"');
}

sub _HttpSendHeader {
	my($self, $sock, %args) = @_;
	$args{'Content-Type'}   = $args{'Content-Type'}   || CTYPE_HTML;
	$args{'Content-Length'} = int($args{'Content-Length'} || 0);
	$args{'Cache-Control'}  = 'no-store, no-cache, must-revalidate, private';
	$args{'Expires'}        = 'Thu, 01 Jan 1970 00:00:00 GMT';
	$args{'Connection'}     = 'close';
	
	my $scode = delete($args{'Scode'});
	my $buff  = '';
	while(my($k,$v) = each(%args)) {
		$buff .= "$k: $v\r\n";
	}
	$buff .= "\r\n";
	
	$self->{super}->Network->WriteDataNow($sock, "HTTP/1.1 $scode NIL\r\n$buff");
	$self->SetSockState($sock,STATE_SENDBODY);
}



##########################################################################
# Accept a POST'ed Torrent file, and dumps it into the autoload directory
sub _JSON_NewTorrentAction {
	my ($self, $torrent) = @_;
	my $ok = 0;
	my $msg = '';
	
	# Create new autoload file
	my $destfile = $self->{super}->Tools->GetExclusiveTempfile;
	
	if( open(DEST, ">", $destfile) ) {
		print DEST $torrent;
		close(DEST);
		my $exe = $self->{super}->Admin->ExecuteCommand('load', $destfile);
		$ok = 1 unless $exe->{FAILS};
		$msg = $exe->{MSG}[0][1];
		unlink($destfile) or $self->panic("Could not remove $destfile : $!");
	}
	else {
		$msg = "Could not create temp file $destfile: $!";
	}
	
	$msg = $self->_sEsc($msg);
	return qq!({ ok => $ok, msg => "$msg" })!;
}

##########################################################################
# Return global statistics
sub _JSON_GlobalStats {
	my($self) = @_;
	
	my $x = { sent=>0, recv=>0, disk_free=>-1, disk_total=>-1, disk_quota=>0 };
	
	# add network stats
	$x->{sent}       = $self->{super}->Network->GetStats->{'sent'};
	$x->{recv}       = $self->{super}->Network->GetStats->{'recv'};
	$x->{disk_quota} = abs(int($self->{super}->Configuration->GetValue('min_free_mb'))*1024*1024);
	
	# add quota info
	my $df = $self->{super}->Syscall->statworkdir;
	if($df) {
		# syscall supported by os
		$x->{disk_free}  = $df->{bytes_free};
		$x->{disk_total} = $df->{bytes_total};
	}
	
	my $js = "{ ".join(", ", map({"\"$_\" : $x->{$_}"} keys(%$x)))." }\n";
	return $js;
}

##########################################################################
# Returns a list of all torrents
sub _JSON_TorrentList {
	my($self) = @_;
	my $qlist = $self->{super}->Queue->GetQueueList;
	my @list = ();
	foreach my $dl_type (sort(keys(%$qlist))) {
		foreach my $key (sort(keys(%{$qlist->{$dl_type}}))) {
			push(@list, $self->_JSON_InfoTorrent($key));
		}
	}
	return '['."\n  ".join(",\n  ",@list)."\n".']'."\n";
}

##########################################################################
# Detailed information about a single torrent
sub _JSON_InfoTorrent {
	my($self, $hash) = @_;
	my %info = ();
	if(my $so = $self->{super}->Storage->OpenStorage($hash)) {
		my $stats = $self->{super}->Queue->GetStats($hash);
		%info = %$stats;
		$info{name}       = substr($so->GetSetting('name'),0,180);
		$info{type}       = $so->GetSetting('type');
		$info{paused}     = $self->{super}->Queue->IsPaused($hash);
		$info{autopaused} = $self->{super}->Queue->IsAutoPaused($hash);
		$info{committing} = 0;
		$info{committed}  = ($so->CommitFullyDone ? 1 : 0);
		$info{eta}        = int($self->{super}->Tools->GetETA($hash) || 0);
		
		if(my $ci = $so->CommitIsRunning) {
			$info{committing} = 1;
			$info{commitinfo} = "Committing file $ci->{file}/$ci->{total_files}, ".int(($ci->{total_size}-$ci->{written})/1024/1024)." MB left";
		}
		
	}
	$info{key} = $hash;
	my $json = "{ ";
	while(my($k,$v) = each(%info)) {
		$json .= "\"".$self->_sEsc($k)."\" : \"".$self->_sEsc($v)."\",";
	}
	chop($json);
	$json .= " }\n";
	return $json;
}

##########################################################################
# List files of given hash
sub _JSON_ShowFiles {
	my($self, $hash) = @_;
	my $r    = $self->{super}->Admin->ExecuteCommand("files", $hash, "list");
	my @list = ();
	foreach my $ar (@{$r->{MSG}}) {
		push(@list, '"'.int($ar->[0])."|".$self->_sEsc($ar->[1]).'"');
	}
	return '['."\n  ".join(",\n  ",@list)."\n".']'."\n";
}

sub _JSON_ShowFilesExtended {
	my($self, $hash) = @_;
	my @list = ();
	if(my $so = $self->{super}->Storage->OpenStorage($hash)) {
		for(my $i=0; $i < $so->GetFileCount; $i++) {
			my $fp_info             = $so->GetFileProgress($i);
			my $this_file           = $fp_info->{finfo};
			   ($this_file->{name}) = $this_file->{path} =~ /\/?([^\/]+)$/; # create a shorthand name
			
			# pollute $fp_info with some additional infos:
			map( { $fp_info->{$_} = $this_file->{$_} } qw(size path name) );
			delete($fp_info->{finfo});
			
			my $json = "{";
			while(my($k,$v) = each(%$fp_info)) {
				$json .= " \"".$self->_sEsc($k)."\" : \"".$self->_sEsc($v)."\",";
			}
			chop($json); # remove last ','
			$json .= " }";
			push(@list, $json);
		}
	}
	return "[\n".join(",\n  ",@list)."\n]\n";
}

##########################################################################
# Show peers of given hash
sub _JSON_ShowPeers {
	my($self, $hash) = @_;
	my $r    = $self->{super}->Admin->ExecuteCommand("peerlist", $hash);
	my @list = ();
	foreach my $ar (@{$r->{MSG}}) {
		push(@list, '"'.$self->_sEsc($ar->[1]).'"') if !defined($ar->[0]) or $ar->[0] != 4;  # XXX: 4 is used for gui stuff
	}
	return '['."\n  ".join(",\n  ",@list)."\n".']'."\n";
}

##########################################################################
# Return all tions starting at given index
sub _JSON_RecvNotify {
	my($self, $start_at) = @_;
	
	my $nx      = $self->{notify};
	my @nbuff   = ( "\"next\" : \"$nx->{end}\" ", "\"first\" : \"$nx->{start}\" ", );
	my $start   = ($start_at < $nx->{start} ? $nx->{start} : $start_at);
	
	for(0..NOTIFY_BUFF) {
		my $q = $start+$_;
		if(exists($nx->{ref}->{$q})) {
			push(@nbuff, " \"$q\" : \"".$self->_sEsc($nx->{ref}->{$q})."\" ");
		}
		else {
			last;
		}
	}
	return ("{".join(',', @nbuff)."}\n")
}


##########################################################################
# Set and get configuration
sub _JSON_ConfigurationAction {
	my($self,$action,$arg) = @_;
	
	my @cbuff = ();
	my $cref  = $self->{super}->Configuration;
	
	if($action eq 'get') {
		my @toget = ($arg eq 'ALL' ? $cref->GetKeys : $arg);
		foreach my $key (@toget) {
			my $val  = $cref->GetValue($key);
			next if !defined($val);
			
			my $lock = ($cref->IsRuntimeLocked($key) ? 1 : 0);
			push(@cbuff, "{ \"key\":\"$key\", \"value\":\"".$self->_sEsc($val)."\", \"locked\":$lock }");
		}
	}
	elsif($action eq 'set' && $arg && $arg =~ /^(\S+)=(.+)$/) {
		my($key,$val) = ($1,$2);
		if(defined($cref->GetValue($key))) {
			$cref->SetValue($key,$val);
			return $self->_JSON_ConfigurationAction('get',$key);
		}
	}
	
	return ("[".join(",\n", @cbuff)."]\n");
}

##########################################################################
# Return  history
sub _JSON_HistoryAction {
	my($self, $args) = @_;
	
	my @hbuff = ();
	
	if($args eq 'list') {
		my $result = $self->{super}->Admin->ExecuteCommand('history', 'list')->{MSG};
		foreach my $aitem (@$result) {
			my $txt = $self->_sEsc($aitem->[1]);
			if($txt =~ /^(\S{40}) : (.*)$/) {
				push(@hbuff, "{ \"id\" : \"$1\" , \"text\" : \"".($2 || "No description ($1)")."\"}");
			}
		}
	}
	elsif($args =~ /forget\/(\S{40})$/) {
		my $result = $self->{super}->Admin->ExecuteCommand('history', $1, 'forget');
		return $self->_JSON_HistoryAction('list');
	}
	
	return ("[".join(",\n", @hbuff)."]\n");
}

##########################################################################
# Create a new torrent
sub _JSON_CreateTorrentAction {
	my($self,$args) = @_;
	
	my $result = $self->{super}->Admin->ExecuteCommand('create_torrent','--name',$args);
	
	my $msg0  = $result->{MSG}->[0];
	my $ok    = ( defined($msg0->[0]) ? 0 : 1);
	my $txt   = $self->_sEsc($msg0->[1]);
	my ($sha) = $txt =~ /\[sha1: (.+)\]$/;
	return "{ ok:$ok , msg:\"$txt\", sha1:\"$sha\"}\n";
}

##########################################################################
# Start a download and (cheap-ass) translate the return msg into a notify
sub _JSON_StartDownload {
	my($self,$uri) = @_;
	my $ret  = $self->{super}->Admin->ExecuteCommand('load',$uri);
	foreach my $x (@{$ret->{MSG}}) {
		$self->{super}->Admin->SendNotify($x->[1]);
	}
	return("()");
}

# ScriptEsc
sub _sEsc {
	my($self, $str) = @_;
	$str =~ tr/\\//d;
	$str =~ s/"/\\"/gm;
	return $self->_hEsc($str);
}

# HtmlEsc
sub _hEsc {
	my($self,$str) = @_;
	$str =~ s/&/&amp;/gm;
	$str =~ s/</&lt;/gm;
	$str =~ s/>/&gt;/gm;
	return $str;
}

1;

package Bitflu::AdminHTTP::Data;

	sub new {
		my($class, %args) = @_;
		my $self = { super => $args{super} };
		bless($self,$class);
		return $self;
	}
	
	
	sub Get {
		my($self,$what) = @_;
		
		if($what eq "/") {
			return('text/html', $self->_Index);
		}
		if($what eq '/bg_blue.png') {
			return('image/png', $self->_BackgroundBlue);
		}
		if($what eq '/bg_white.png') {
			return('image/png', $self->_BackgroundWhite);
		}
		if($what eq '/bg_lblue.png') {
			return('image/png', $self->_BackgroundLBlue);
		}
		if($what eq '/ic_back.png') {
			return('image/png', $self->_IconBack);
		}
		if($what eq '/ic_dir.png') {
			return('image/png', $self->_IconDir);
		}
		if($what eq '/ic_file.png') {
			return('image/png', $self->_IconFile);
		}
		return ('text/plain', "requested url '$what' was not found on this server\n");
	}
	
	
	sub _BackgroundBlue {
		my $b = '';
		$b .= pack("H*", "89504e470d0a1a0a0000000d49484452000000180000001808040000004a7ef57300000002624b474400ff878fccbf000000097048597300000b1300000b1301");
		$b .= pack("H*", "009a9c180000000774494d4507d70c1e0c161dc05d6bf00000001d74455874436f6d6d656e7400437265617465642077697468205468652047494d50ef64256e");
		$b .= pack("H*", "0000003c4944415438cbedd0a11180401443c125fafaef83dece83c14306fb5dc48acc3bce0b16d8de773abea5e3a4e3cfa5ef7c49c749c7a7d2549a4abf2add");
		$b .= pack("H*", "8ddd60ec0bf1bd7c0000000049454e44ae426082");
		return $b;
	}
	
	sub _BackgroundWhite {
		my $b = '';
		$b .= pack("H*", "89504e470d0a1a0a0000000d4948445200000018000000180806000000e0773df8000000097048597300000b1300000b1301009a9c180000000774494d4507d6");
		$b .= pack("H*", "07170911317a8bfe010000001d74455874436f6d6d656e7400437265617465642077697468205468652047494d50ef64256e000000524944415448c7edd2210e");
		$b .= pack("H*", "c0300c4351b7d8f7bf68cc3dd00d8ccf432e8a0a7ea4e8ad9931ee47f21921095ffcef645cd259908a03c0b2ed54fc75a2449ce459908a03c04ec6aba88aaaa8");
		$b .= pack("H*", "8aaaa88a7e537401e3fd3097f29339740000000049454e44ae426082");
		return $b;
	}
	
	sub _BackgroundLBlue {
		my $b = '';
		$b .= pack("H*", "89504e470d0a1a0a0000000d4948445200000018000000180806000000e0773df800000006624b474400ff00ff00ffa0bda793000000097048597300000b1300");
		$b .= pack("H*", "000b1301009a9c180000000774494d4507d70c1e0c2130158597b10000001d74455874436f6d6d656e7400437265617465642077697468205468652047494d50");
		$b .= pack("H*", "ef64256e000000534944415448c7edd2210e80301044d14feddcff707008dc6810058167aaa66a53f137d9bc6d3fce8be7497a476cf3c7ff48c66dcf05a938c0");
		$b .= pack("H*", "48c63f274ac425cd05a938c048c6aba88aaaa88aaaa88a9629ba0118152100188797ae0000000049454e44ae426082");
		return $b;
	}
	
	sub _IconBack {
		my $b = '';
		$b .= pack("H*","89504e470d0a1a0a0000000d494844520000000c0000000c080600000056755ce7000000017352474200aece1ce90000000774494d4507d90b08150801eeb225");
		$b .= pack("H*","bc00000006624b474400ff00ff00ffa0bda793000000097048597300000b1300000b1301009a9c18000000314944415428cf6360c00dfe433151e03f291afe93");
		$b .= pack("H*","a2e13f119824c5ff49554cbe060672343090a381e460252be2f0260d00ca4a4db3c09d54b50000000049454e44ae426082");
		return $b;
	}
	
	sub _IconDir {
		my $b = '';
		$b .= pack("H*","89504e470d0a1a0a0000000d494844520000000c0000000d08060000009d298f42000000017352474200aece1ce90000000774494d4507d90b0815170d2a5e67");
		$b .= pack("H*","0900000006624b4744000000000000f943bb7f000000097048597300000b1300000b1301009a9c18000000374944415428cf6364c00d9880f81fba202394fe8f");
		$b .= pack("H*","439c81580d38d593aa411cab86a6a62686baba3a300d03203e39368c6a204903d100009e321224383afc050000000049454e44ae426082");
		return $b;
	}
	
	sub _IconFile {
		my $b = '';
		$b .= pack("H*","89504e470d0a1a0a0000000d494844520000000c0000000d08060000009d298f42000000017352474200aece1ce90000000774494d4507d90b081512194df347");
		$b .= pack("H*","3100000006624b4744000000000000f943bb7f000000097048597300000b1300000b1301009a9c180000003b4944415428cf6360c00d98f0c831fc2712233410");
		$b .= pack("H*","025835343535c131321f498318e536a0db84660395fc80cd3fd4b3019b5fa86303a9314d3400009453290d7b9050bb0000000049454e44ae426082");
		return $b;
	}
	
	sub _Index {
		my($self) = @_;
		
#		open(X,"/home/adrian/src/bitflu/web.html");
#		my $buff = join("",<X>);
#		close(X);
#		return $buff;
		
		my $buff = << 'EOF';
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>

<meta http-equiv="content-type" content="text/html; charset=utf-8">
<title>Welcome to Bitflu</title>


<!-- Config:
http://developer.yahoo.com/yui/articles/hosting/?base&button&connectioncore&container&containercore&datasource&datatable&dom&event&fonts&grids&json&layout&menu&progressbar&reset&resize&tabview&yahoo&MIN&loadOptional&nocombine&basepath&http://ajax.googleapis.com/ajax/libs/yui/2.8.2r1/build/&google
//-->

<!-- Combo-handled YUI CSS files: -->
<link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/combo?2.8.2r1/build/reset-fonts-grids/reset-fonts-grids.css&2.8.2r1/build/base/base-min.css&2.8.2r1/build/assets/skins/sam/skin.css">
<!-- Combo-handled YUI JS files: -->
<script type="text/javascript" src="http://yui.yahooapis.com/combo?2.8.2r1/build/utilities/utilities.js&2.8.2r1/build/container/container-min.js&2.8.2r1/build/menu/menu-min.js&2.8.2r1/build/event-mouseenter/event-mouseenter-min.js&2.8.2r1/build/selector/selector-min.js&2.8.2r1/build/event-delegate/event-delegate-min.js&2.8.2r1/build/button/button-min.js&2.8.2r1/build/datasource/datasource-min.js&2.8.2r1/build/calendar/calendar-min.js&2.8.2r1/build/paginator/paginator-min.js&2.8.2r1/build/datatable/datatable-min.js&2.8.2r1/build/json/json-min.js&2.8.2r1/build/resize/resize-min.js&2.8.2r1/build/layout/layout-min.js&2.8.2r1/build/progressbar/progressbar-min.js&2.8.2r1/build/tabview/tabview-min.js"></script>


<style type="text/css">
html {
	background: #f4f4f4;
}
body {
	margin:0;
	padding:0;
}
div.yuimenu .bd {
	zoom: normal;
}
label { 
	display:block;
	float:left;
	width:45%;
	clear:left;
}
.clear {
	clear:both;
}
#details_widget_dtable thead {
  display: none;
} 

.bfheading {
	background: url(http://yui.yahooapis.com/2.8.2r1/build/assets/skins/sam/sprite.png) repeat-x 0 -1400px;
	border: 1px solid gray;
	text-align: left;
	font-size: 14px;
	font-weight: bold;
	color: white;
	padding: 2px 0px 4px;
}

</style>
</head>


<body class="yui-skin-sam">
<script>
	
	
	function get_icon(name,size) {
		if(!size)
			size = 16;
		
		var url = "http://tiny.cdn.eqmx.net/icons/gnome/16x16";
		var xmap = { paused:"/actions/media-playback-pause.png", running:"/actions/mail-send-receive.png", done:"/actions/document-save.png", msg:"/actions/help-about.png" };
		return "<img width="+size+" height="+size+" src='"+url+xmap[name]+"'>";
	}
	
	function sec_to_human(str) {
		var sec = parseInt(str);
		if(sec < 1)
			return "&infin;";
		if(sec < 5)
			return "-";
		if(sec < 60)
			return (sec/1).toFixed(0)+" sec";
		if(sec < 60*90)
			return (sec/60).toFixed(0)+" min";
		if(sec < 3600*48)
			return (sec/3600).toFixed(1)+ "h";
		if(sec < 86400*10)
			return (sec/86400).toFixed(1)+ "d";
		if(sec < 86400*31)
			return (sec/86400/7).toFixed(1)+ "w";
		
		return "&gt; 4w";
	}
	
	function upload_hack(el) {
		if(mview.startdl_widge) {
			mview.startdl_widget.hide();
		}
	}
	
	/* Setup lefthandside menu */
	var fmenu = {
		init: function(where){
		var w_fmenu = new YAHOO.widget.Menu("basicmenu", { visible:true, position:"static", shadow:false });
		w_fmenu.addItems([
		{text:"Browse"        ,url:"browse/", target:"_new" },
		{text:"Start download",onclick:{ fn: function(){mview.startdl_widget.show()  } } },
		{text:"Create torrent",onclick:{ fn: function(){mview.mktorrent_widget.show()} } },
		{text:"Notifications <span id=\"notify_icon\"></span>", onclick:{ fn: function(){mview.notify_widget.show()} } },
		{text:"History"       ,onclick:{ fn: function(){rpcsrv.history()  }}},
		{text:"Configuration" ,onclick:{ fn: function(){rpcsrv.configuration()}}},
		{text:"Help"          ,url:"http://bitflu.workaround.ch/httpui-help.html", target:"_new" },
		{text:"About"         ,onclick:{ fn: function(){mview.about_widget.show()    } } },
		
		]);
		w_fmenu.render(where);
	}};
	
	
	
	// non-modal panels should call olmanager.register();
	var olmanager = new YAHOO.widget.OverlayManager(); 
	
	var mview    = {
	                 about_widget:{}, download_table:{}, startdl_widget:{}, details_widget:{}, cancel_widget:{}, files_widget:{},
	                 stats_widget:{}, mktorrent_widget:{}, notify_widget:{}, history_widget:{}, configuration_widget:{}, dfbar_widget:{},
	               };
	
	
	var rpcsrv = {
		pause : function(a,b,args) {
			var xaction = (args[1] ? 'resume' : 'pause');
			YAHOO.log("pause: "+xaction+" "+args[0]);
			YAHOO.util.Connect.asyncRequest('GET',xaction+"/"+args[0], { success: mview.download_table.refresh } );
		},
		cancel : function(a,b,args) {
			var xaction = (args[1] ? 'wipe' : 'cancel');
			YAHOO.log("cancel: "+xaction+" "+args[0]);
			YAHOO.util.Connect.asyncRequest('GET',xaction+"/"+args[0], { success: mview.download_table.refresh } );
		},
		cdialog: function(a,b,args) {
			mview.cancel_widget.show(args[0],args[1],args[2]);
		},
		details : function(a,b,qid) {
			YAHOO.util.Connect.asyncRequest('GET',"info/"+qid, { success: function(o) { mview.details_widget.show(o); } } );
		},
		files : function(a,b,qid) {
			YAHOO.util.Connect.asyncRequest('GET','showfiles-ext/'+qid, { success: function(o) { mview.files_widget.show(o,qid); } });
		},
		history : function() {
			YAHOO.util.Connect.asyncRequest("GET","history/list", { success: function(o) { mview.history_widget.show(o); } } );
		},
		forget  : function(a,b,qid) {
			YAHOO.util.Connect.asyncRequest("GET", "history/forget/"+qid, { success: function(o) { rpcsrv.history() } });
		},
		configuration : function() {
			YAHOO.util.Connect.asyncRequest("GET","configuration/get/ALL", { success: function(o) { mview.configuration_widget.show(o); } } );
		},
		configsave : function(k,v,dt) {
			dt.disable();
			YAHOO.util.Connect.asyncRequest("GET","configuration/set/"+k+"="+v, {success: function() { dt.undisable() } });
		},
		inexclude : function(qid,fid,exclude,dt) {
			dt.disable();
			var upart = (exclude ? "exclude" : "include");
			YAHOO.util.Connect.asyncRequest("GET", upart+"/"+qid+"/"+fid, {success: function() { dt.undisable() } });
		}
	};
	
	/*********************************************************************************************************
	** Fill contextmenu before showing it
	**********************************************************************************************************/
	var ctx_before_show = function(stype, p_aArgs,dtobj) {
		
		this.clearContent();
		
		var this_target = this.contextEventTarget;
		var this_tr     = this_target.nodeName.toUpperCase() == "TR" ? this_target : YAHOO.util.Dom.getAncestorByTagName(this_target,"TR");
		var rset        = dtobj.getRecordSet().getRecord(this_tr.id);
		
		var is_paused   = (rset.getData("paused")=="0" ? false: true);
		var qid         = rset.getData("id");
		var txt         = rset.getData("name");
		var iscommitted = (rset.getData("committed") == "0" ? false : true);
		this.addItems([ [{text:rset.getData("name").substr(0,64), disabled:true}],
		  [{text:"Show details", onclick:{fn:rpcsrv.details,obj:qid } },
		   {text:"Show files",   onclick:{fn:rpcsrv.files  ,obj:qid } },
		   {text:"Browse",       url:"getfile/"+qid+"/browse/", target:"_new"},
		   {text:"Paused",       onclick:{fn:rpcsrv.pause,  obj:[qid,is_paused]}, checked:is_paused},
		   {text:"Remove",       onclick:{fn:rpcsrv.cdialog,obj:[qid,txt,iscommitted] } },
		
		]]);
		
		this.render();
	
	}
	
	/*********************************************************************************************************
	** Manage 'diskfree' bar
	**********************************************************************************************************/
	var create_dfbar_widget = function(t) {
		t.obj    =  pbar = new YAHOO.widget.ProgressBar({value:0, height: 4, width:140, maxValue:100, minValue:0})
		t.render = function(where) { t.obj.render(where) }
		t.set    = function(val) {
			t.obj.set('value',val);
		}
	}
	
	/*********************************************************************************************************
	** Download Table view
	**********************************************************************************************************/
	var create_download_table = function(t) {
		
		t.pbar_list   = []; // progress bars to destroy before rendering data
		t.dsource     = []; // current datasource data
		
		/* callback for the progress bars */
		t.pbar_fmt    = function(elLiner, oRecord, oColumn, oData) {
			var pb = new YAHOO.widget.ProgressBar({ width:'140px', height:'14px', maxValue:100,value:parseInt(oData)}).render(elLiner);
			t.pbar_list.push(pb);
		}
		
		/* callback for the icons-formatter */
		t.icon_fmt = function(el, rec, col, dat)  {
			var paused = rec.getData("paused");
			var commit = rec.getData("committed");
			var sstr   = (commit != "0" ? "done"    : "running");
			sstr       = (paused == "0" ? sstr      : "paused");
			el.innerHTML=get_icon(sstr);
		}
		
		
		/* refresh callback: take current dsdata and update the UI (unless disabled) */
		t.refresh_cb = {
			timeout: 3000,
			success: function(o) {
				YAHOO.log("Writing new datasource data");
				var data = [];
				try {
					data = YAHOO.lang.JSON.parse(o.responseText);
				}
				catch(x) {
					YAHOO.log("Error parsing JSON for torrentList"+o.responseText);
					return;
				}
				
				/* update current datasource */
				t.dsource = [];
				for(var i=0, len=data.length; i<len; i++) {
					var tx  = data[i];
					var pct = ( (tx.done_bytes+1)/(tx.total_bytes+1)*100 ).toFixed(1);
					var eta = sec_to_human(tx.eta);
					t.dsource.push({name:tx.name.substr(0,50), prog:pct, id:tx.key, done:(tx.done_bytes/1024/1024).toFixed(1) + "/" + (tx.total_bytes/1024/1024).toFixed(1),
					                peers:tx.active_clients+"/"+tx.clients, up: (tx.speed_upload/1024).toFixed(1), down: (tx.speed_download/1024).toFixed(1),
					                ratio: (tx.uploaded_bytes/(1*tx.done_bytes+1)).toFixed(2), state:" ", paused:tx.paused,
					                committed:tx.committed, eta:eta,
					});
				}
				t.obj.getDataSource().sendRequest(null, {success: t.obj.onDataReturnInitializeTable}, t.obj);
				
				YAHOO.util.Dom.setStyle("shdiv", "height", 300+parseInt(YAHOO.util.Dom.getStyle(t.obj, 'height'),10)+"px");
			}
		}
		
		/* triggers a datasource and ui update */
		t.refresh = function() {
			YAHOO.log("refreshing torrent list");
			YAHOO.util.Connect.asyncRequest('GET',"torrentList", t.refresh_cb);
		}
		
		/* create the actual table */
		t.obj = new YAHOO.widget.DataTable("download_table", [
			{key:"state", label:" ",         resizeable:false, formatter:t.icon_fmt },
			{key:"name",  label:"Name",      resizeable:true  },
			{key:"prog",  label:"Progress",  resizeable:false, formatter:t.pbar_fmt  },
			{key:"done",  label:"Done (MB)", resizeable:false  },
			{key:"ratio", label:"Ratio",     resizeable:false  },
			{key:"peers", label:"Peers",     resizeable:false  },
			{key:"up",    label:"Up",        resizeable:false  },
			{key:"down",  label:"Down",      resizeable:false  },
			{key:"paused",label:"Paused",    resizeable:false, hidden:true },
			{key:"id"  ,  label:"QueueID",   resizeable:false, hidden:true },
			{key:"committed",label:"Commited",resizeable:false, hidden:true},
			{key:"eta",  label:"ETA",        resizeable:false  },
		], new YAHOO.util.DataSource(function(){ return t.dsource }), {MSG_EMPTY:"No downloads running"});
		
		t.ctxmenu = new YAHOO.widget.ContextMenu("ctx_menu", { trigger:"download_table", lazyload:true, zindex:999 });
		t.ctxmenu.subscribe("beforeShow", ctx_before_show, t.obj); 
		
		
		/* tell YUI to destroy all old progress bars before re-rendering the table */
		t.obj.on('beforeRenderEvent',function() {
			for (var i = 0; i<t.pbar_list.length; i++) { t.pbar_list[i].destroy(); }
			t.pbar_list = [];
		});
		
		
		/* get initial data and set refresher */
		t.refresh();
		window.setInterval(t.refresh, 3000);
		
		/* standdard functions */
		t.show = function() {
			YAHOO.util.Dom.setStyle('download_table', 'visibility', ''); 
		};
		t.hide = function() {
			YAHOO.util.Dom.setStyle('download_table', 'visibility', 'hidden'); 
		};
		
	}
	
	var create_mktorrent_widget = function(t) {
		
		t.mktorrent = function(x) {
			var name = x.getData().mktorrent_name;
			t.hide();
			x.form.reset();
			t.show_wait(name);
			YAHOO.log("mktorrent: "+name);
			YAHOO.util.Connect.asyncRequest('GET',"createtorrent/"+name, { timeout: 1000*60*30, success:function(){t.hide_wait()} });
		}
		
		t.wait_window = new YAHOO.widget.Panel("mktorrent_wait", { width:"300px", visible:false, draggable:true,close:false,modal:true,fixedcenter:true});
		t.wait_window.setHeader("Creating torrent, please wait");
		t.wait_window.render("modal_dialogs");
		
		t.show_wait = function(desc) {
			t.wait_window.setBody("Creating<br><b>"+desc+"</b><br>please wait...");
			t.wait_window.show();
		}
		
		t.hide_wait = function() {
			t.wait_window.hide();
		}
		
		t.obj = new YAHOO.widget.Dialog("mktorrent_widget",
		          { width:"600px", visible:false, draggable:true, close:true, fixedcenter:true, modal:true,
		            buttons: [ {text:"Create torrent", handler:function(){t.mktorrent(this)}, isDefault:true},
		                       {text:"Abort"         , handler:function(){t.hide()         }                },
		                     ]
		           });
		t.obj.render();
		
		t.show = function() { t.obj.show();   }
		t.hide = function() { t.obj.cancel(); }
	}
	
	var create_stats_widget = function(t) {
		t.obj = '';
		t.refresh_cb = {
			timeout: 3000,
			success: function(o) {
				var data = {};
				try {
					data = YAHOO.lang.JSON.parse(o.responseText);
				}
				catch(x) {
					YAHOO.log("Error parsing JSON "+x);
					return;
				}
				window.document.title = "Up: " + (data.sent/1024).toFixed(2) + " KiB/s | Down: " + (data.recv/1024).toFixed(2) + " KiB/s  -  Bitflu";
				
				if(data.disk_total > 0) {
					var free = data.disk_free - data.disk_quota;
					free     = ( free < 1 ? 1 : free );
					
					var pct    = (100-(free/data.disk_total)*100).toFixed(1);
					pct        = (pct > 100 ? 100 : pct);
					
					mview.dfbar_widget.set(parseInt(pct));
					document.getElementById("df_txt").innerHTML = "Diskusage: "+pct+"%";
				}
				
			}
		};
		t.refresh = function() {
			YAHOO.util.Connect.asyncRequest('GET',"stats",t.refresh_cb);
		}
		window.setInterval(t.refresh, 3700);
	}
	
	/*********************************************************************************************************
	** Information widget
	**********************************************************************************************************/
	var create_about_widget = function(t) {
		
		t.obj  = new YAHOO.widget.Panel("about_panel",{ width:"320px", visible:false, constraintoviewport:true } ); 
		t.obj.setHeader("About Bitflu");
		t.obj.setBody("<div style='background: #110011; font-size: 28px; color: white;'><font size=62><i><b>Bitflu</b></i></font><br>$$VERSION$$</div><hr>\
		(C) <a href=mailto:adrian@blinkenlights.ch>Adrian Ulrich</a><br><br>Homepage: <a target=_new href=http://bitflu.workaround.ch>http://bitflu.workaround.ch</a>");
		t.obj.render("multi_dialogs");
		
		t.show = function() {
			YAHOO.log("Showing info widget");
			t.obj.show();
			t.obj.focus();
		}
		t.hide = function() {
			YAHOO.log("Hiding info widget");
			t.obj.cancel();
		}
	}
	
	
	var create_notify_widget = function(t) {
		
		t.nid = 0; /* notification id to get */
		
		t.obj = new YAHOO.widget.Panel("notify_widget", { visible:false, draggable:true, width:"800px", height:"220px", close:true,
		                                                  constraintoviewport: true, modal: false  });
		
		t.obj.setHeader("Notifications");
		t.obj.setBody("<div id='notify_div' style='width:100%; height:100%; overflow: scroll; font-family: monospace; font-size: 12px; text-align:left;'></div>");
		t.obj.render("notify_dialog");

		t.resize =   new YAHOO.util.Resize('notify_widget', { handles: ['br'], autoRatio: false, minWidth: 500, minHeight: 220, status: false });
		t.resize.on('resize', function(args) {
			this.cfg.setProperty("height", args.height + "px");
		}, t.obj, true);
		
		
		t.nicon = document.getElementById("notify_icon");
		t.show  = function() { t.nicon.innerHTML=""; t.obj.show(); t.obj.focus(); }
		t.hide  = function() { t.obj.cancel()}
		
		t.refresh_cb = {
			timeout: 3000,
			success : function(o) {
				var data = {};
				try {
					data = YAHOO.lang.JSON.parse(o.responseText);
				}
				catch(x) {
					YAHOO.log("Error parsing JSON "+x);
					return;
				}
				
				var ndiv    = document.getElementById("notify_div");
				var updated = 0;
				
				if(t.nid < data.first) {
					t.nid  = data.first;
					updated--;
				}
				
				for(var i=t.nid;i<data.next;i++) {
					var c = ( i%2 == 0 ? "e9e9e9" : "dadada");
					var d = "<div style='margin: 3px;background-color: #"+c+";'>"
					ndiv.innerHTML = d+data[i]+"</div>"+ndiv.innerHTML;
					if(updated >= 0) { updated++ }
				}
				
				t.nid = data.next;
				
				/* add icon to notify text */
				if(updated > 0 && t.obj.cfg.getProperty("visible")==false) {
					t.nicon.innerHTML = get_icon("msg",13);
				}
				
			}
		}
		
		t.refresh = function() {
			YAHOO.log("Recify "+t.nid);
			YAHOO.util.Connect.asyncRequest('GET',"recvnotify/0",t.refresh_cb);
		}
		
		window.setInterval(t.refresh, 5000);
		
	}
	
	/*********************************************************************************************************
	** Start-Download dialog
	**********************************************************************************************************/
	var create_cancel_widget = function(t) {
		t.key = '';
		
		t.obj =  new YAHOO.widget.SimpleDialog("cancel_widget", {
			  fixedcenter:true, visible:false, modal:true, draggable:true,
			  width:"600px", constraintoviewport:true,
			  buttons: [
			    {text:"Yes",                      handler:function(){rpcsrv.cancel(0,0,[t.key,0]);t.hide();}},
			    {text:"Yes, and remove all data", handler:function(){rpcsrv.cancel(0,0,[t.key,1]);t.hide();}},
			    {text:"No",                       handler:function(){t.hide();}, isDefault:true}
			  ],
			   });
		t.obj.render("modal_dialogs");
		
		t.hide = function() {
			t.obj.cancel();
		}
		t.show = function(key,desc, iscom) {
			t.key = key;
			
			var msg = "Do you want to remove<br><b>"+desc+"</b><br>from the download queue?";
			if(!iscom)
				msg = msg+"<br><br><b>Warning: This download is not committed! Hitting 'Yes' will remove downloaded data!</b><br><br>";
			
			t.obj.setHeader("Remove "+t.key+" ?");
			t.obj.setBody(msg);
			
			t.obj.show();
		}
	}
	/*********************************************************************************************************
	** Start-Download dialog
	**********************************************************************************************************/
	var create_startdl_widget = function(t) {
		
		t.submit = function(xf) {
			var uri  = xf.new_uri.value;
			YAHOO.log("Loading "+uri);
			YAHOO.util.Connect.asyncRequest('GET',"startdownload/"+uri, function(){});
			t.hide();
			xf.reset();
		}
		
		t.tab = new YAHOO.widget.TabView('startdl_tab_widget');
		t.obj =  new YAHOO.widget.Dialog("startdl_widget", 
					{ width: "600px", visible:false, draggable:true, close:true, fixedcenter:true, modal:true,
					  buttons: [ { text:"Close", handler:function(){t.hide()}} ]
					});
		t.obj.setHeader("Start download");
		t.obj.render();
		
		t.show = function() { t.obj.show();  }
		t.hide = function() { t.obj.cancel();}
	}
	
	
	/*********************************************************************************************************
	** Configuration dialog
	**********************************************************************************************************/
	var create_configuration_widget = function(t) {
		t.obj = new YAHOO.widget.Dialog("configuration_widget_panel",{ width:"740px", visible:false, close:true,constraintoviewport:true,
		         buttons:[ {text:"Close", isDefault:true, handler:function(){t.hide()}}]});
		t.obj.setHeader("Configuration");
		t.obj.setBody("<div id=\"configuration_widget_dtable\"></div>");
		t.obj.render("multi_dialogs");
		
		t.dsource = [];
		t.editor  = new YAHOO.widget.TextboxCellEditor();
		t.dtobj   = new YAHOO.widget.ScrollingDataTable("configuration_widget_dtable",
		                 [{key:"key", label:"Name"},{key:"value", label:"Value", editor: t.editor},{key:"locked",value:"L",hidden:true}],
		                 new YAHOO.util.DataSource(function() { return t.dsource }),
		                 {width:"100%", height:"100%"} );
		
		/* Setup CellEditor */
		t.dtobj.subscribe("cellClickEvent", function(args) {
			var record = this.getRecord(args.target);
			if(record.getData("locked") == "0") {
				this.onEventShowCellEditor(args)
			}
		});
		
		t.editor.subscribe("saveEvent", function(args) {
			var rset = this.getRecord(this.getId);
			var key  = rset.getData("key");
			var val  = rset.getData("value");
			setTimeout(function() { rpcsrv.configsave(key,val,t.dtobj); }, 0);
		});
		
		add_resizer(t, "configuration_widget_panel");
		
		t.show  = function(j){ t.fill(j); t.obj.show(); t.obj.focus();}
		t.hide  = function() { t.obj.hide() }
		
		t.fill = function(json) {
			t.dsource = [];
			var data  = [];
			try      { data = YAHOO.lang.JSON.parse(json.responseText) }
			catch(x) { YAHOO.log("Error: "+x);return; }
			
			for(i in data) {
				var entry = data[i];
				var fcol_s= ( entry.locked ? "<font color=grey>" : "" );
				var fcol_e= ( entry.locked ? "</font>"           : "" );
				
				t.dsource.push({key:fcol_s+entry.key+fcol_e, value:entry.value, locked:entry.locked});
			}
			t.size(400);
			t.dtobj.getDataSource().sendRequest(null, {success: t.dtobj.onDataReturnInitializeTable},t.dtobj);
		}
		
	}
	
	var create_files_widget = function(t) {
		t.obj = new YAHOO.widget.Dialog("files_widget_panel",{ width:"740px", visible:false, modal:true, fixedcenter:true, close:true,constraintoviewport:true,
		         buttons:[ {text:"Close", isDefault:true, handler:function(){t.hide()}}]});
		t.obj.setHeader(" ");
		t.obj.setBody("<div id=\"files_widget_dtable\"></div>");
		t.obj.render("modal_dialogs");
		
		t.qid     = false;
		t.dsource = [];
		
		t.editor  = new YAHOO.widget.DropdownCellEditor({dropdownOptions:["Normal","Exclude"],disableBtns:true});
		t.dtobj   = new YAHOO.widget.ScrollingDataTable("files_widget_dtable", [{key:"path", label:"Path"},{key:"size", label:"Size (MB)", parser:"number"},
		                 {key:"prog", label:"Progress"},{key:"action",label:"Status",editor:t.editor},
		                 { key:"link", label:"Download"},{key:"fid", hidden:true}],
		                 new YAHOO.util.DataSource(function() { return t.dsource }), {width:"100%", height:"100%"} );
		t.dtobj.on('editorSaveEvent', function() { YAHOO.util.Dom.setStyle(t.dtobj.getTableEl(),'width','100%')});
		t.dtobj.on('initEvent',       function() { YAHOO.util.Dom.setStyle(t.dtobj.getTableEl(),'width','100%')});
		t.dtobj.subscribe("cellClickEvent", t.dtobj.onEventShowCellEditor); 
		
		t.editor.subscribe("saveEvent", function(args) {
			var rset = this.getRecord(this.getId);
			var fid  = rset.getData("fid");
			var excl = (rset.getData("action") == "Exclude" ? 1 : 0);
			setTimeout(function() { rpcsrv.inexclude(t.qid,fid,excl,t.dtobj) });
		});
		
		add_resizer(t,"files_widget_panel");
		
		t.show = function(j,q){ t.qid=q; t.fill(j); t.obj.show(); t.obj.focus();}
		t.hide = function()   { t.obj.hide() }
		
		t.fill = function(json) {
			t.dsource = [];
			var data  = [];
			try      { data = YAHOO.lang.JSON.parse(json.responseText) }
			catch(x) { YAHOO.log("Error: "+x); return; }
			
			for(i in data) {
				var entry = data[i];
				var fid   = 1+parseInt(i);
				t.dsource.push({
					path:entry.path,
					prog:(entry.done/entry.chunks*100).toFixed(2)+"%",
					size:(entry.size/1024/1024).toFixed(2),
					action:(entry.excluded==0 ? "Normal" : "Exclude"),
					link:"<a href='getfile/"+t.qid+"/"+fid+"'>Link</a>",
					fid:fid,
				});
			}
			
			t.size(400);
			t.obj.setHeader("Files of "+t.qid);
			t.dtobj.getDataSource().sendRequest(null, {success: t.dtobj.onDataReturnInitializeTable},t.dtobj);
		}
		
	}
	
	/*********************************************************************************************************
	** History dialog
	**********************************************************************************************************/
	var create_history_widget = function(t) {
		t.obj = new YAHOO.widget.Dialog("history_widget_panel",{ width:"740px", visible:false, close:true,constraintoviewport:true,
		         buttons:[ {text:"Close", isDefault:true, handler:function(){t.hide()}}]});
		t.obj.setHeader("Download history");
		t.obj.setBody("<div id=\"history_widget_dtable\"></div>");
		t.obj.render("multi_dialogs");
		
		t.dsource = [];
		t.dtobj   = new YAHOO.widget.ScrollingDataTable("history_widget_dtable", [{key:"name", label:"Name"},{key:"hash", label:"Hash"},{key:"action",label:"Action"}],
		                 new YAHOO.util.DataSource(function() { return t.dsource }), {width:"100%", height:"100%"} );
		
		t.dtobj.on('initEvent', function() { YAHOO.util.Dom.setStyle(t.dtobj.getTableEl(),'width','100%')});
		
		add_resizer(t,"history_widget_panel");
		
		t.show = function(j){ t.fill(j); t.obj.show(); t.obj.focus();}
		t.hide = function() { t.obj.hide() }
		
		
		
		t.fill = function(json) {
			t.dsource = [];
			var data  = [];
			try      { data = YAHOO.lang.JSON.parse(json.responseText) }
			catch(x) { YAHOO.log("Error: "+x); return; }
			
			for(i in data) {
				var entry = data[i];
				t.dsource.push({name:entry.text.substr(0,35), hash:entry.id,
				                action:"<button onClick=\"rpcsrv.forget(0,0,'"+entry.id+"')\">Forget</button>"});
			}
			t.size(400);
			t.dtobj.getDataSource().sendRequest(null, {success: t.dtobj.onDataReturnInitializeTable},t.dtobj);
		}
		
	}
	
	/************************************************************ 
	 * Downloads-Detail widget
	 ***********************************************************/
	var create_details_widget = function(t) {
		
		t.obj  = new YAHOO.widget.Dialog("details_widget_panel",{  width:"600px",visible:false, modal:true, close:false,fixedcenter:true,constraintoviewport:true,
		         buttons: [ {text:"Close", isDefault:true, handler:function(){t.hide()} } ],
		 } ); 
		t.obj.render();
		
		t.dsource = [];
		t.dtobj = new YAHOO.widget.DataTable("details_widget_dtable", [{key:"key"},{key:"val"}],
		                 new YAHOO.util.DataSource(function(){ return t.dsource }), {width:"50 px"});
		t.dtobj.on('initEvent', function() { YAHOO.util.Dom.setStyle(t.dtobj.getTableEl(),'width','100%')});
		
		t.show = function(j){ t.fill(j); t.obj.show(); t.obj.focus(); }
		t.hide = function() { t.obj.cancel()}
		
		t.fill = function(json) {
			var data = [];
			try      { data = YAHOO.lang.JSON.parse(json.responseText) }
			catch(x) { return; }
			
			t.obj.setHeader("Details for "+data.key);
			t.dsource =
			[
			   {key:"Name", val:data.name},
			   {key:"Hash", val:data.key},
			   {key:"Network", val:data.type},
			   {key:"Total size (MB)", val:(data.total_bytes/1024/1024).toFixed(2)},
			   {key:"Done (MB)", val:(data.done_bytes/1024/1024).toFixed(2)}, {key:"Uploaded (MB)", val:(data.uploaded_bytes/1024/1024).toFixed(2)},
			   {key:"Pieces", val:"Got "+data.done_chunks+" out of "+data.total_chunks}, {key:"Status", val:(data.committed=="1" ? "Finished" : "Not finished")},
			   {key:"Paused", val:(data.paused=="1" ? "Yes" : "No"),},
			];
			t.dtobj.getDataSource().sendRequest(null, {success: t.dtobj.onDataReturnInitializeTable},t.dtobj);
			t.dtobj.selectRow(0);
		}
	}
	
	/************************************************************ 
	 * Create all widget objects
	 ***********************************************************/
	function boot_widgets(target) {
		YAHOO.log("Booting widgets from "+target);
		for(var x in target) {
			YAHOO.log("booting "+x);
			eval('create_'+x+'(target.'+x+');');
			
			if(target[x].obj && target[x].obj["cfg"] && target[x].obj.cfg.getProperty("modal") == false) {
				YAHOO.log("Adding to overlay manager: "+x);
				olmanager.register(target[x].obj);
			}
			
		}
	}
	
	function add_resizer(t,name) {
		t.resize = new YAHOO.util.Resize(name, { handles: ['br'], autoRatio: false, minWidth: 740, maxWidth: 740, minHeight: 100,status: false });
		t.resize.on('resize', function(args) { t.size(args.height);}, t.obj, true);
		t.size = function(height) {
			t.obj.cfg.setProperty("height", height+"px");
			YAHOO.util.Dom.setStyle(t.dtobj,"height", height-100+"px");
		}
		return t.resize;
	}
	
	/************************************************************ 
	 * Init javascript stuff
	 ***********************************************************/
	function init() {
		fmenu.init("filter_menu");
/*		
		new YAHOO.widget.LogReader(null,
		 {footerEnabled: false, verboseOutput:false, draggable:true,
		  top: "340px", left:true, width:"700px", newestOnTop:false});         // add debug windo
*/		
		boot_widgets(mview);
		mview.download_table.show();
		mview.dfbar_widget.render("df_pbar");
	}
	
	YAHOO.util.Event.addListener(window, "load", init);
</script>





<div id="top_menu" style="text-align: right; padding: 10px 50px 10px;">
		<form onSubmit="mview.startdl_widget.submit(this); return false;">
			<input type="text" name="new_uri" size=50>
			<input type="submit" name="startdl_btn" value="Start download"></input>&nbsp;&nbsp;&nbsp;
		</form>
</div>

<div id="shdiv" style="width: 1px; height:0px; float:left;"></div>

<div style="margin-left: 4px; width: 140px; float: left;">

<div style="border: 1px grey solid">
<div class="bfheading">&nbsp;</div>
<div id="filter_menu"></div>
</div>
<br>

<div id="df_txt" style="font-size: 10px">&nbsp;</div>
<div id="df_pbar"></div>

</div>



<div style="margin-left: 150px;">
<div class="bfheading">&nbsp;Download queue</div>
<div id="download_table" style="visibility:hidden"></div>
</div>








<div id="mktorrent_widget" class="yui-pe-content">
<div class="hd">Create new .torrent file</div>
<div class="bd">
<form>
<b>Name:</b>	<input type="text" name="mktorrent_name" size=50>
</form>
<br>
Place your content into <b>$$IMPORTDIR$$</b> and hit 'Create Torrent'<br>
<br>
<b>Note:</b> Bitflu will block until the process finished. This can take a long time if you import large amounts of data!
</div>
</div>


<div id="startdl_widget">
	<div id="startdl_tab_widget" class="yui-navset">
		<ul class="yui-nav" style="background: #cecece; text-align:middle;">
			<li class="selected"><a href="#tab1"><em>Load URL</em></a></li>
			<li><a href="#tab2"><em>Upload Torrent</em></a></li>
		</ul>
		<div class="yui-content">
			<div id="tab1">
			<br>
			Please enter the location of the new file that bitflu should download.
			<br>
			This can be an http://, dht:// a magnet-link or a local file on the server (such as: /tmp/foo.torrent)
			<div class="clear"></div>
			<br>
			<form onSubmit="mview.startdl_widget.submit(this)">
				<input type="text" name="new_uri" size=50>
				<input type="submit" name="startdl_btn" value="Start download"></input>
			</form>
			<br><br>
			</div>
			<div id="tab2">
			<br>
			<form method="POST" enctype='multipart/form-data' action='new_torrent_httpui' target="new_torrent_if">
				<iframe width=0 height=0 style="visibility:hidden" onLoad="upload_hack(this)" name="new_torrent_if"></iframe>
				
				Select a torrent file on your local computer:<br><br><input type="file" name="torrent"><br><br>
				and hit <input type="submit" value="Upload torrent"><br>
				<br>
			</form>
			</div>
		</div>
	</div>
</div>






<!-- righthandside menu //-->
<div id="ctx_menu"></div>
<div id="modal_dialogs"></div>
<div id="multi_dialogs" style="position: absolute; top: 80px; left: 30%"></div>
<div id="notify_dialog"></div>
<div id="details_widget_panel">
			<div class="hd"></div>
			<div class="bd"><div id="details_widget_dtable"></div></div>
</div>




</body>
</html>
EOF

	my $thisvers = $self->{super}->GetVersionString;
	my $impdir   = Bitflu::AdminHTTP::_hEsc(undef, ($self->{super}->Configuration->GetValue("torrent_importdir")));
	$buff =~ s/\$\$VERSION\$\$/$thisvers/gm;
	$buff =~ s/\$\$IMPORTDIR\$\$/$impdir/gm;
	return($buff);
}
	
	
	
1;
