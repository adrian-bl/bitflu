package Bitflu::AdminHTTP;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2008-2009 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use POSIX;
use constant _BITFLU_APIVERSION => 20091125;

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
	
	
	
	my $sock = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$xconf->{webgui_port}, Bind=>$xconf->{webgui_bind},
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
	if($rq =~ /^(GET|HEAD|POST) (\/\S*)/) {
		$ref->{METHOD} = uc($1);
		$ref->{URI}    = $2;
	}
	return $ref;
}


##########################################################################
# Overview with all downloads
sub CreateToplevelDirlist {
	my($self) = @_;
	my $qlist = $self->{super}->Queue->GetQueueList;
	
	my $buff  = qq{<html><head><title>Bitflu Downloads</title></head><body background="/bg_white.png"><h2 style="background:url(/bg_lblue.png);">
		Download overview</h2><table border=0>};
	
	foreach my $dl_type (sort(keys(%$qlist))) {
		foreach my $key (sort(keys(%{$qlist->{$dl_type}}))) {
			my $dlso   = $self->{super}->Storage->OpenStorage($key) or $self->panic;
			my $stats  = $self->{super}->Queue->GetStats($key)      or $self->panic;
			my $dlname = $self->_hEsc($dlso->GetSetting('name'));
			my $done   = ($stats->{done_chunks}/$stats->{total_chunks})*100;
			   $done   = 99.9 if $done == 100 && $stats->{done_chunks} != $stats->{total_chunks}; # round
			   $done   = sprintf("%5.1f%%",$done);

			$buff .= "<tr><td align=right>$done</td><td>&nbsp;</td><td><img src=/ic_dir.png></td><td><a href='../getfile/$key/browse/'>$dlname</a></td></tr>\n";
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
	my $desc   = $self->_hEsc($so->GetSetting('name').$filter);
	
	# Build dirlist.
	# Filter ALWAYS starts with a '/' and ends with / if we are in dirlist mode
	
	my $dl_dirs  = {};
	my $dl_files = {};
	for(my $i=0; $i < $so->GetFileCount; $i++) {
		my $this_file = $so->GetFileInfo($i);
		my $path      = "/".$this_file->{path};
		if( $filter =~ /\/$/ && (my ($rest) = $path =~ /^\Q$filter\E(.+)/) ) {
			if($rest =~ /^([^\/]+)\//) { $dl_dirs->{$1}++;       }
			else                       { $dl_files->{$rest} = $i;}
		}
		elsif($filter eq $path) {
			return('',$self->GuessContentType($path),$i+1); # cannot be 0 -> consistency with GUI
		}
	}
	
	my $total_done = ($stats->{done_chunks}/$stats->{total_chunks})*100;
	   $total_done = 99.9 if $total_done == 100 && $stats->{done_chunks} != $stats->{total_chunks}; # round
	   $total_done = sprintf("%5.1f%%",$total_done);
	
	my $buff = qq{<html><head><title>Index of $desc</title></head><body background="/bg_white.png"><h2 style="background:url(/bg_lblue.png);">
		[$total_done] Index of: $desc</h2><table border=0 width=100%>};
	
	$buff .= "<tr><td width=14></td><td></td><td width=120></td></tr>\n";
	
	if($filter eq '/') { # not at toplevel dir -> add backlink (we are at: getfile/hash/browse/
		$buff .= "<tr><td><img src=/ic_back.png></td><td><a href='../../../browse/'>&nbsp;Overview</a></td><td></td></tr>\n";
	}
	else {
		$buff .= "<tr><td><img src=/ic_back.png></td><td><a href='../'>&nbsp;Back</a></td><td></td></tr>\n";
	}
	
	$buff .= "<tr><td></td><td></td><td></td></tr>\n";
	
	foreach my $dirname (sort(keys(%$dl_dirs))) {
		my $si  = $dl_dirs->{$dirname};
		my $txt = "subitem".($si==1?'':'s');
		$buff .= "<tr><td><img src=/ic_dir.png></td><td><a href=\"".$self->{super}->Tools->UriEscape($dirname)."/\">";
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
		$buff .= "<tr title='$percent%'><td><img src=/ic_file.png></td><td>";
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
	my $destfile = Bitflu::Tools::GetExclusivePath(undef, $self->{super}->Configuration->GetValue('workdir')."/".$self->{super}->Configuration->GetValue('tempdir'));
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
	return "({ \"sent\" : \"".$self->{super}->Network->GetStats->{'sent'}."\", \"recv\" : \"".$self->{super}->Network->GetStats->{'recv'}."\" })\n";
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
		$info{committing} = 0;
		$info{committed}  = ($so->CommitFullyDone ? 1 : 0);
		
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
	return "( $json )";
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
	return ("({".join(',', @nbuff)."})\n")
}

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
	
	return ("([".join(",\n", @hbuff)."])\n");
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
		my $buff = << 'EOF';
<html>
<head>
<title>Loading... - Bitflu</title>

<style type="text/css">
	BODY {
		font-family: Helvetica, Arial, sans-serif;
		font-size: 12px;
		background: url("bg_blue.png");
	}
	
	table {
		border: 0px;
		border-spacing: 0px;
		border-collpase:collpase;
	}
	
	.flist {
		max-height: 400px;
		overflow: auto;
	}
	
	.bitfluBanner {
		background: url("bg_lblue.png");
		position: absolute;
		border: solid #000000;
		padding: 4px;
		bottom: 0px;
		left: 0px;
	}
	
	.pWindow {
		background: url("bg_lblue.png");
		font-weight:bold;
		cursor : move;
		white-space : nowrap;
	}
	
	.pWindowNoCursor {
		background: url("bg_lblue.png");
		font-weight:bold;
	}
	
	.cButton {
		font-weight:bold;
		font-size: 8px;
		background: #cecece;
		border: 1px black solid;
		margin-right: 2px;
		margin-top: 2px;
		width: 16px;
		text-align: right;
	}
	
	.tTable {
		background: url("bg_white.png");
		padding: 5px;
	}
	
	.dlHeader {
		font-weight: bold;
	}
	
	
	.dlStalled {
		color: #33383d;
		cursor:pointer;
	}
	
	.dlRunning {
		color: #014608;
		cursor:pointer;
	}
	
	.dlDead {
		color: #6a0202;
		cursor:pointer;
	}
	
	.dlCommitted {
		background-color: #dbe1b1;
		cursor:pointer;
	}
	
	.dlComplete {
		background-color: #c9d4e7;
		cursor:pointer;
	}
	
	.dlPaused {
		background-color: #e0e0e0;
		font-style:       italic;
		cursor:           pointer;
	}
	
	.xButton {
		font-weight:bold;
		width:18px;
		height:20px;
		border-style: none;
	}

	.pbBorder {
		height: 12px;
		width: 205px;
		background: url("bg_white.png");
		border: 1px solid silver;
		margin: 0px;
		padding: 1px;
	}

	.pbFiller {
		height: 12px;
		margin: 0px;
		font-size: 10px;
		padding: 0;
		color: #333333;
		opacity: 0.8;
	}
	
	.xMaintable {
		background: url("xwhiter.png");
		width: 100%;
		top: 0px;
	}
	
	.yyTable0 {
		background: url("bg_lblue.png");
	}
	
	.yyTable1 {
		background: url("xwhiter.png");
	}
	
	.mBar a {
		text-decoration: none;
		padding: 4px;
		color: #5a5a5a;
		font-weight: bold;
		font-size: 14px;
	}
	.mBar a:hover {
		text-decoration: none;
		padding: 4px;
		color: #333333;
		font-weight: bold;
		font-size: 14px;
	}


	.xNav li {
		display:inline;
		padding: 0;
		margin: 0;
	}
	.xNav li a {
		text-decoration: none;
		padding: 4px;
		color: #333333;
		font-weight: bold;
		font-size: 14px;
	}
	.xNav li a:hover {
		color: #222222;
		text-decoration: underline;
		background: url("darker.png");
	}


</style>

<script language="JavaScript">

var refreshable   = new Array();
var moving_window = 0;
var mouse_off_y   = 0;
var mouse_off_x   = 0;
var mouse_now_y   = 0;
var mouse_now_x   = 0;
var top_z_num     = 0;
var notify_index  = 0;
var notify_ack    = 0;
var banner_tout   = null;

function reqObj() {
	var X;
	try { X=new XMLHttpRequest(); }
	catch (e) {
		try { X=new ActiveXObject("Microsoft.XMLHTTP"); }
		catch (e) {
			try { X=new ActiveXObject("Msxml2.XMLHTTP.3.0"); }
			catch (e) {
				alert("Your browser does not support AJAX!");
				return false;
			}
		}
	}
	return X;
}

function getZindex() {
	return (top_z_num++);
}

function removeDialog(id) {
	delete refreshable[id];
	document.body.removeChild(document.getElementById("window_" + id));
}

function showBannerWindow(text) {
	clearTimeout(banner_tout);
	var e = document.getElementById("bitfluBanner");
	var o = (e.style.display=='' ? e.innerHTML : '');
	    o = (o.length > 0 ? o+'<hr>' : '');
	e.innerHTML = o + text;
	e.style.display = '';
	banner_tout = window.setTimeout(function() { hideBannerWindow() },5000);
}

function hideBannerWindow() {
	clearTimeout(banner_tout); /* just in case... */
	document.getElementById("bitfluBanner").style.display='none';
}

function clearBannerWindow() {
	document.getElementById("bitfluBanner").innerHTML = '';
}

function displayHistory(key) {
	var window = document.getElementById("content_internal-history");
	if(window) {
		document.getElementById("title_internal-history").innerHTML     = "Download History";
		document.getElementById('window_internal-history').style.zIndex = getZindex();
		
		var x = new reqObj();
		x.onreadystatechange=function() {
			if(x.readyState == 4) {
				var hist = eval(x.responseText);
				var i    = 0;
				delete x['onreadystatechange'];
				x = null;
				
				var content = "<table>";
				for (i=0; i<hist.length; i++) {
					var this_obj = hist[i];
					var this_key = this_obj['id'];
					var this_txt = this_obj['text'].substr(0,80);
					content += "<tr class=yyTable"+(i%2?1:0)+"><td title='queue id: "+this_key+"'>"+this_txt+"</td>";
					content += "<td><button onClick='javascript:displayHistory(\""+this_key+"\")'>Remove</button></td></tr>";
				}
				content += "</table>";
				
				if(i == 0) {
					content = "<b>History is empty</b>";
				}
				
				window.innerHTML = content;
			}
		}
		
		
		if(key) { x.open("GET", "history/forget/"+key) }
		else    { x.open("GET", "history/list")      }
		
		x.send(null);
	}
	else {
		addJsonDialog(0, "internal-history", 'History');
		displayHistory();
	}
}

function displayAbout() {
	var window = document.getElementById("content_internal-about");
	if(window) {
		document.getElementById('title_internal-about').innerHTML     = "About Bitflu";
		document.getElementById('window_internal-about').style.zIndex = getZindex();
		var xtxt         = "<table><tr><td>Version</td><td>$$VERSION$$</td></tr>";
		    xtxt        += "<tr><td>Contact</td><td><a href='mailto:adrian\@blinkenlights.ch'>adrian\@blinkenlights.ch</a></td></tr>";
		    xtxt        += "<tr><td>Website</td><td><a href='http://bitflu.workaround.ch' target='_new'>http://bitflu.workaround.ch</a></td></tr>";
		    xtxt        += "</table>";
		window.innerHTML = xtxt;
	}
	else {
		addJsonDialog(0, 'internal-about', 'About Bitflu');
		displayAbout();
	}
}


function displayUpload() {
	var window = document.getElementById("content_internal-upload");
	if(window) {
		document.getElementById('title_internal-upload').innerHTML     = "Upload Torrent file";
		document.getElementById('window_internal-upload').style.zIndex = getZindex();
		var xtxt         = "<form method='POST' action='/new_torrent_httpui' enctype='multipart/form-data' target='newtorrent'>";
		    xtxt        += "<iframe onLoad='removeDialog(\"internal-upload\");' name='newtorrent' src='#' style='width:0;height:0;border:0px solid #fff;'></iframe>";
		    xtxt        += "<input type=file id=torrent name=torrent size=40><br><input type='submit' value='Upload'>";
		    xtxt        += "</form>";
		window.innerHTML = xtxt;
	}
	else {
		addJsonDialog(0, 'internal-upload', 'Upload Torrent file');
		displayUpload();
	}
}


function addJsonDialog(xfunc, key, title) {
	var xexists = '';
	if(xexists = document.getElementById("window_"+key)) {
		xexists.style.zIndex = getZindex();
		return false;
	}
	var element            = document.createElement('div');
	var content            = '';
	element.id             = "window_"+key;
	element.className      = 'tTable';
	element.style.top      = mouse_now_y;
	element.style.left     = mouse_now_x;
	element.style.position = 'absolute';
	element.style.border   = '2px solid #001100';
	element.style.zIndex   = getZindex();
	content += "<div class=pWindow OnMouseDown=\"dragON('"+key+"')\"><div id=\"title_"+key+"\"><i>Loading...</i></div></div>";
	content += "<p id=\"content_"+key+"\"><i>Loading...</i></p>";
	content += "<div style=\"position:absolute;top:0;right:0;cursor:default;\">";
	if(xfunc) {
		content += "<button onClick=\"refreshable['" +key+ "']='updateDetailWindow';refreshInterface(0);\"><b>&lt;</b></button>";
	}
	content += "<button onClick=\"removeDialog('" + key + "')\" class=cButton >x</div>";
	element.innerHTML      = content;
	document.body.appendChild(element);
	
	if(xfunc) {
		refreshable[key] = ""+xfunc;
	}
	refreshInterface(0);
}


function dragON(key) {
	moving_window        = key;
	var element          = document.getElementById("window_" + moving_window);
	var moving_at_x      = parseInt(element.style.left);
	var moving_at_y      = parseInt(element.style.top);
	mouse_off_x          = mouse_now_x - moving_at_x;
	mouse_off_y          = mouse_now_y - moving_at_y;
	element.style.zIndex = getZindex();
}

function dragOFF() {
	moving_window = 0;
}

function dragItem(e) {
	mouse_now_y = e.clientY;
	mouse_now_x = e.clientX;
	
	if(!moving_window) {
		return false;
	}
	var element = document.getElementById("window_" + moving_window);
	var Y       = mouse_now_y - mouse_off_y;
	var X       = mouse_now_x - mouse_off_x;
	
	element.style.top = (Y > 0 ? Y : 0);
	element.style.left =(X > 0 ? X : 0);
}

function startDownloadFrom(xid) {
	var e = document.getElementById(xid);
	var x = new reqObj();
	x.onreadystatechange=function()	{
		if (x.readyState == 4) {
			delete x['onreadystatechange'];
			x = null;
		}
	}
	
	if(e.value == '') {
		showBannerWindow('No URL specified');
	}
	else {
		showBannerWindow('Starting download...');
	}
	
	// Sending an empty GET request doesn't hurt..
	x.open("GET", "startdownload/"+e.value,true);
	x.send(null);
	e.value = '';
}

function updateNotify(enforced) {
	var x = new reqObj();
	x.onreadystatechange=function()	{
		if (x.readyState == 4 && x.status == 200) {
			var noti = eval(x.responseText);
			if(noti && (enforced || noti["next"] != notify_index)) {
				notify_index = noti["next"];
				var x_html     = '';
				var notify_cnt = 0;
				for(var i=(notify_index-1); i>=noti["first"]; i--) {
					if(i >= notify_ack) {
						x_html += noti[i] + "<br>";
						notify_cnt++;
					}
				}
				
				if(!enforced && notify_cnt == 0) {
					/* Nothing to display */
				}
				else {
					clearBannerWindow();
					showBannerWindow('<div class=pWindow>Notifications</div>'+ (x_html.length == 0 ? '<i>notify list is empty</i>' : x_html) +
					                 '<div style="position:absolute;top:0;right:0;cursor:default;"><button onClick="hideBannerWindow()" class=cButton>x</button></div>' );
				}
			}
			delete x['onreadystatechange'];
			x = null;
		}
	}
	x.open("GET", "recvnotify/0", true); /* Recv ALL tions */
	x.send(null);
}

function updateTorrents() {
	var x = new reqObj();
	x.onreadystatechange=function()	{
		if (x.readyState == 4 && x.status == 200) {
			var t_array = eval(x.responseText);
			var t_html  = '<table width="100%" class=tTable>';
			    t_html += "<tr class=dlHeader><td>Name</td><td>Progress</td><td>Done (MB)</td><td>Ratio</td><td>Peers</td><td>Up</td><td>Down</td><td></td></tr>";
			for(var i=0; i<t_array.length; i++) {
				var t_obj    = t_array[i];
				var t_id     = t_obj['key'];
				var t_style  = 'dlStalled';
				var t_bgcol  = '#6688ab';
				var t_sstate = 'Pause';
				var percent  = ( (t_obj['done_bytes']+1)/(t_obj['total_bytes']+1)*100).toFixed(1);
				var onclick  = "onClick=\"addJsonDialog('updateDetailWindow', '" +t_id+"','loading')\"";
				if(t_obj['paused'] == 1)                                { t_style = 'dlPaused';    t_bgcol='#898989'; t_sstate = 'Resume';}
				else if(t_obj['committed'] == 1)                        { t_style = 'dlCommitted'; t_bgcol='#447544'}
				else if(t_obj['done_chunks'] == t_obj['total_chunks'] ) { t_style = 'dlComplete';  }
				else if(t_obj['active_clients'] > 0)                    { t_style = 'dlRunning';   }
				else if(t_obj['clients'] == 0)                          { t_style = 'dlDead';      }
				
				
				t_html += "<tr class="+t_style+" id='item_" + t_id + "' title='queue id: "+t_id+"'>";
				t_html += "<td "+onclick+">" + t_obj['name'] + "</td>";
				t_html += "<td "+onclick+"><div class=pbBorder><div class=pbFiller style=\"background-color:"+t_bgcol+";width: "+percent+"%\"></div></div></td>";
				t_html += "<td "+onclick+">" + (t_obj['done_bytes']/1024/1024).toFixed(1) + "/" + (t_obj['total_bytes']/1024/1024).toFixed(1) + "</td>";
				t_html += "<td "+onclick+">" + (t_obj['uploaded_bytes']/(1*t_obj['done_bytes'] + 1)).toFixed(2)  + "</td>";
				t_html += "<td "+onclick+">" + t_obj['active_clients'] + "/" + t_obj['clients'] + "</td>";
				t_html += "<td "+onclick+">" + (t_obj['speed_upload']/1024).toFixed(1) + "</td>";
				t_html += "<td "+onclick+">" + (t_obj['speed_download']/1024).toFixed(1) + "</td>";
				t_html += "<td>" + '<button  onclick="_rpc' + t_sstate + '(\''+t_obj['key']+'\')">' + t_sstate + '</button>';
				t_html += "</tr>";
			}
			t_html += "</table>";
			document.getElementById("tlist").innerHTML = t_html;
			delete x['onreadystatechange'];
			x = null;
		}
	}
	x.open("GET", "torrentList", true);
	x.send(null);
}

function updateStats() {
	var x = new reqObj();
	x.onreadystatechange=function() {
		if (x.readyState == 4 && x.status == 200) {
			var stats = eval(x.responseText);
			var xup   = stats['sent']/1024;
			var xdown = stats['recv']/1024;
			var udtxt = "Up: " + xup.toFixed(2) + " KiB/s | Down: " + xdown.toFixed(2) + " KiB/s";
			window.defaultStatus  = udtxt;
			window.document.title = udtxt + " - Bitflu";
			delete x['onreadystatechange'];
			x = null;
		}
	}
	x.open("GET", "stats", true);
	x.send(null);
}

function updateDetailWindow(key) {
	var element = document.getElementById("content_" + key);
	var x = new reqObj();
	x.onreadystatechange=function() {
		if (x.readyState == 4 && x.status == 200) {
			var t_info = eval(x.responseText);
			var t_html = '<table>';
			    t_html += '<tr class=yyTable0><td>Name</td><td>' + t_info['name'] + '</td></tr>';
			    t_html += '<tr class=yyTable1><td>Network</td><td>' + t_info['type'] + '</td></tr>';
			    t_html += '<tr class=yyTable0><td>Downloaded</td><td>' + (t_info['done_bytes']/1024/1024).toFixed(2) + ' MB ('+t_info['done_chunks']+'/'+t_info['total_chunks']+' pieces)</td></tr>';
			    t_html += '<tr class=yyTable1><td>Uploaded</td><td>'   + (t_info['uploaded_bytes']/1024/1024).toFixed(2) + 'MB</td></tr>';
			    t_html += '<tr class=yyTable0><td>Peers</td><td>' +t_info['clients']+' peers connected, '+t_info['active_clients']+' of them are active</td></tr>';
			    t_html += '<tr class=yyTable1><td>Committed</td><td>' + (t_info['committed'] == 1 ? 'Yes' : 'No') + '</td></tr>';
			    t_html += '<tr class=yyTable0><td>Commit running</td><td>' + (t_info['committing'] == 1 ? 'Yes: '+t_info['commitinfo'] : 'No') + '</td></tr>';
			    t_html += '</table>';
			    if(t_info['paused'] == 1) {
			       t_html += '<button onclick="_rpcResume(\''+t_info['key']+'\')">Resume</button>';
			    }
			    else {
			       t_html += '<button onclick="_rpcPause(\''+t_info['key']+'\')">Pause</button>';
			    }
			    t_html += '<button onclick="confirmCancel(\''+t_info['key']+'\')">Cancel</button>';
			    t_html += '<button onclick="_rpcShowFiles(\''+t_info['key']+'\')">Show Files</button>';
					t_html += '<button onclick="window.open(\'getfile/'+t_info['key']+'/browse/\',\'_blank\');">Browse</button>';
			    t_html += '<button onclick="_rpcPeerlist(\''+t_info['key']+'\')">Display Peers</button>';
			delete x['onreadystatechange'];
			x = null;
			if(refreshable[key] == 'updateDetailWindow') {
				element.innerHTML = t_html;
				document.getElementById("title_" + key).innerHTML = t_info['name'];
			}
		}
	}
	x.open("GET", "info/"+key, true);
	x.send(null);
}

function confirmCancel(key) {
	delete refreshable[key]; // Do not trigger UI updates
	var element = document.getElementById("content_" + key);
	var t_html =  "Are you sure?<hr>";
	    t_html += '<button onclick="removeDialog(\''+key+'\')">No</button> ';
	    t_html += '<button onclick="_rpcCancel(\''+key+'\')">Yes, cancel it</button>';
	element.innerHTML = t_html;
}

function _rpcCancel(key) {
	var x = new reqObj();
	x.open("GET", "cancel/"+key, true);
	x.send(null);
	removeDialog(key);
	refreshInterface(1);
}

function _rpcExcludeFile(key, file, exclude) {
	var x = new reqObj();
	x.open("GET", (exclude ? 'exclude' : 'include') + "/" + key + "/" + file, true);
	x.send(null);
	refreshNow(key);
}

function _rpcPause(key) {
	var x = new reqObj();
	x.open("GET", "pause/"+key, true);
	x.send(null);
	refreshInterface(1);
}

function _rpcResume(key) {
	var x = new reqObj();
	x.open("GET", "resume/"+key, true);
	x.send(null);
	refreshInterface(1);
}

function _rpcShowFiles(key) {
	refreshable[key] = '_rpcShowFiles';
	var element = document.getElementById("content_" + key);
	var x = new reqObj();
	x.onreadystatechange=function() {
		
		if (x.readyState == 4 && x.status == 200) {
			var t_info = eval(x.responseText);
			var t_html = '<div class="flist"><table cellpadding=2>';
			
			for(var i=0; i < t_info.length; i++) {
				
				var splitted  = t_info[i].split('|');
				var inex_txt  = 'Loading';
				var inex_cls  = 'dlRunning';
				var inex_val  = 1;
				
				if(splitted[0] != 0) {
					inex_txt = 'Excluded';
					inex_cls = 'dlStalled';
					inex_val = 0;
				}
				
				var this_line = '<tr class="' + inex_cls+ '" '+(i%2!=0 ? 'style="background: url(bg_lblue.png);"' : '') +'>';
				
				for(var j=1; j < splitted.length; j++) {
					this_line += '<td>' + splitted[j] + '</td>';
				}
				
				var tosplit = this_line.replace(/\|/g, "</td><td>");
				var t_link  = '<td>Get file</td><td>Status</td>';
				
				if(i > 0) {
					t_link =  '<td><a href=getfile/'+key+'/'+(i)+'>download</a></td>';
					t_link += '<td><button onclick="_rpcExcludeFile(\''+ key +'\', '+ i +', ' + inex_val +')">' + inex_txt +'</button></td>';
				}
				
				t_html += this_line + t_link + "</tr>\n";
			}
			t_html += "</table></div>\n";
			delete x['onreadystatechange'];
			x = null;
			if(refreshable[key] == '_rpcShowFiles') {
				element.innerHTML = t_html;
			}
		}
	}
	
	x.open("GET", "showfiles/"+key, true);
	x.send(null);
}

function _rpcPeerlist(key) {
	refreshable[key] = '_rpcPeerlist';
	var element = document.getElementById("content_" + key);
	var x = new reqObj();
	x.onreadystatechange=function() {
		if (x.readyState == 4 && x.status == 200) {
			var t_info = eval(x.responseText);
			var t_html = '<table>';
			for(var i=0; i < t_info.length; i++) {
				var tosplit = t_info[i].replace(/\|/g, "</td><td>");
				t_html += "<tr><td>" + tosplit + "</td></tr>\n";
			}
			t_html += "</table>\n";
			delete x['onreadystatechange'];
			x = null;
			if(refreshable[key] == '_rpcPeerlist') {
				element.innerHTML = t_html;
			}
		}
	}
	x.open("GET", "peerlist/"+key, true);
	x.send(null);
}

function refreshInterface(gui) {
	
	if(gui) {
		updateTorrents();
		updateStats();
		updateNotify(0);
	}
	
	for(var i in refreshable) {
		refreshNow(i);
	}
}

function refreshNow(name) {
	var code = refreshable[name] + "('" + name +"')";
	eval(code);
}

function initInterface() {
	showBannerWindow("<i>Welcome to Biflu $$VERSION$$</i>");
	setTimeout('refreshInterface(1)', 1);
	setInterval('refreshInterface(1)', 5000);
}

</script>

</head>
<body onLoad="initInterface()" onMouseMove="dragItem(event)" onMouseUp="dragOFF()">


<div class="bitfluBanner" id="bitfluBanner"></div>



<table cellspacing="2" cellpadding="2" width="100%" class="mBar">
<tr>
	<td><a href="browse/" target="_new">Browse</a></td>
	<td><a href="javascript:updateNotify(1);">Notifications</a></td>
	<td><a href="javascript:displayHistory(0)">History</a></td>
	<td n><a href="javascript:displayUpload()"><nobr>Upload Torrent</nobr></a><td>
	<td><a href="javascript:displayAbout()">About</a></td>
	<td><a href="http://bitflu.workaround.ch/httpui-help.html" target="_new">Help</a></td>
	<td width="100%" align=right><input type="text" id="urlBar" size="40"> <button onClick="startDownloadFrom('urlBar')">Start download</button></td>
</tr>
</table>
<hr>

<p id="tlist" class="tTable">
<i>Loading download list...</i>
</p>




</body>
</html>
EOF

	my $thisvers = $self->{super}->GetVersionString;
	$buff =~ s/\$\$VERSION\$\$/$thisvers/gm;
	return($buff);
	}
	
	
	
1;
