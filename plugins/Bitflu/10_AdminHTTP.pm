package Bitflu::AdminHTTP;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2008 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use POSIX;

use constant STATE_READHEADER => 1;
use constant STATE_SENDBODY   => 2;
use constant SOCKET_TIMEOUT   => 8;
use constant BUFF_MAXSIZE     => 1024*64;

##########################################################################
# Register this plugin
sub register {
	my($class,$mainclass) = @_;
	my $self = { super => $mainclass, sockets => {}, data_dp => Bitflu::AdminHTTP::Data->new };
	bless($self,$class);
	
	$self->{http_port}    = 4081;
	$self->{http_bind}    = '127.0.0.1';
	
	foreach my $funk qw(http_port http_bind) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		if(defined($this_value)) {
			$self->{$funk} = $this_value;
		}
		else {
			$mainclass->Configuration->SetValue($funk,$self->{$funk});
		}
		$mainclass->Configuration->RuntimeLockValue($funk);
	}
	
	
	
	my $sock = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$self->{http_port}, Bind=>$self->{http_bind},
	                                             MaxPeers=>10, Callbacks =>  {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	unless($sock) {
		$self->stop("Unable to bind to $self->{http_bind}:$self->{http_port} : $!");
	}
	
	$self->info(" >> HTTP plugin ready, visit http://$self->{http_bind}:$self->{http_port}");
	$mainclass->AddRunner($self);
#	$mainclass->Admin->RegisterNotify($self, "_Receive_Notify");
	return $self;
}

##########################################################################
# Register  private commands
sub init {
	my($self) = @_;
	return 1;
}

##########################################################################
# Own runner command
sub run {
	my($self) = @_;
	$self->{super}->Network->Run($self);
	
	foreach my $socknam ($self->GetSockets) {
		my $sockstat = $self->GetSockState($socknam);
		my $sockglob = $self->{sockets}->{$socknam}->{socket};
		
		if($sockstat == STATE_SENDBODY) {
			my $qlen   = $self->{super}->Network->GetQueueLen($sockglob);
			my $qfree  = $self->{super}->Network->GetQueueFree($sockglob);
			my $stream = $self->GetStreamJob($sockglob);
			
			if($qlen != 0) {
				# Void, unsent data..
			}
			elsif(defined($stream->{sid}) && (my $so = $self->{super}->Storage->OpenStorage($stream->{sid}))) {
				my($buff,undef) = $so->RetrieveFileChunk($stream->{file}, $stream->{chunk});
				if(defined($buff)) {
					$self->AdvanceStreamJob($sockglob);
					for(my $x = 0; $x<length($buff); $x+=POSIX::BUFSIZ) {
						$self->{super}->Network->WriteDataNow($sockglob,substr($buff,$x,POSIX::BUFSIZ));
					}
				}
				else {
					$self->DropStreamJob($sockglob);
				}
			}
			else {
				$self->DropConnection($sockglob);
			}
		}
	}
	
}

##########################################################################
# Handles a full HTTP request
sub HandleHttpRequest {
	my($self, $sock) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	my $rq = $self->BufferToHttpHeader($sock);
	
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
	
	if($rq->{GET} eq '/torrentList') {
		$data = $self->_JSON_TorrentList;
	}
	elsif($rq->{GET} eq '/stats') {
		$data = $self->_JSON_GlobalStats;
	}
	elsif($rq->{GET} =~ /^\/info\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_InfoTorrent($1);
	}
	elsif($rq->{GET} =~ /^\/cancel\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('cancel', $1);
	}
	elsif($rq->{GET} =~ /^\/pause\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('pause', $1);
	}
	elsif($rq->{GET} =~ /^\/resume\/([a-z0-9]{40})$/) {
		$self->{super}->Admin->ExecuteCommand('resume', $1);
	}
	elsif($rq->{GET} =~ /^\/showfiles\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_ShowFiles($1);
	}
	elsif($rq->{GET} =~ /^\/peerlist\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_ShowPeers($1);
	}
	elsif(my($xh,$xfile) = $rq->{GET} =~ /^\/getfile\/([a-z0-9]{40})\/(\d+)$/) {
		if(my $so = $self->{super}->Storage->OpenStorage($xh)) {
			my $clen   = $so->RetrieveFileSize($xfile);
			my ($fnam) = $so->RetrieveFileName($xfile) =~ /([^\/]+)$/;
			$fnam = $self->_sEsc($fnam);
			$self->HttpSendOkStream($sock, 'Content-Length'=>$clen, 'Content-Disposition' => 'attachment; filename="'.$fnam.'"', 'Content-Type'=>'binary/octet-stream');
			$self->AddStreamJob($sock,$xh,$xfile) if $clen > 0;
		}
		else {
			$self->HttpSendNotFound($sock);
		}
		return;
	}
	else {
		($ctype, $data) = $self->Data->Get($rq->{GET});
	}
	
	$self->HttpSendOk($sock, Payload => $data, 'Content-Type' => $ctype);
}






##########################################################################
# Read data from network
sub _Network_Data {
	my($self,$sock,$buffref, $len) = @_;
	
	my $state = $self->GetSockState($sock);
	
	if($state == STATE_READHEADER) {
		$self->AddSockBuff($sock,$buffref,$len);
		my ($buff,$bufflen) = $self->GetSockBuff($sock);
		if($bufflen >= 4 && substr($buff,-4,4) eq "\r\n\r\n") {
			$self->SetSockState($sock, STATE_SENDBODY);
			$self->HandleHttpRequest($sock);
		}
	}
	else {
		$self->warn("<$sock>: Ignoring data in state $state");
	}
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
	                              timeout_at => $self->{super}->Network->GetTime+SOCKET_TIMEOUT };
	$self->DropStreamJob($sock);
}

##########################################################################
# Remove a socket
sub RemoveSocket {
	my($self, $sock) = @_;
	delete($self->{sockets}->{$sock}) or $self->panic("Unable to remove <$sock>, did not exist?!");
}

##########################################################################
# Register Streaming-Job
sub AddStreamJob {
	my($self,$sock,$sid,$file) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	$sr->{stream} = { sid=>$sid, file=>$file, chunk=>0 };
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
	
	if($sr->{bufflen} <= BUFF_MAXSIZE) {
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
	my($self,$sock) = @_;
	my ($buff, undef) = $self->GetSockBuff($sock);
	my $ref   = {GET=>'/'};
	
	my @lines = split(/\r\n/,$buff);
	my $rq    = shift(@lines);
	
	foreach my $tag (@lines) {
		if($tag =~ /^([^:]+): (.*)$/) {
			$ref->{lc($1)} = $2;
		}
	}
	if($rq =~ /^GET (\/\S*)/) {
		$ref->{GET} = $1;
	}
	return $ref;
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

sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self)."[web]: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self)." [web]: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self)."[web]: ".$msg); }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self)."[web]: ".$msg); }
sub stop { my($self, $msg) = @_;  $self->{super}->stop(ref($self)."[web]: ".$msg); }


####################################################################################################################################################
####################################################################################################################################################
# HTTP Header Stuff


sub HttpSendOk {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>200, 'Content-Length'=>length($args{Payload}), 'Content-Type'=>$args{'Content-Type'});
	$self->{super}->Network->WriteDataNow($sock, $args{Payload});
}

sub HttpSendOkStream {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>200, 'Content-Length'=>$args{'Content-Length'},
	                              'Content-Disposition' => $args{'Content-Disposition'}, 'Content-Type'=>$args{'Content-Type'});
}

sub HttpSendNotFound {
	my($self, $sock, %args) = @_;
	$self->_HttpSendHeader($sock, Scode=>404);
}

sub HttpSendUnauthorized {
	my($self, $sock) = @_;
	$self->_HttpSendHeader($sock, Scode=>401, 'WWW-Authenticate' => 'Basic realm="Bitflu"');
}

sub _HttpSendHeader {
	my($self, $sock, %args) = @_;
	$args{'Content-Type'}   = $args{'Content-Type'}   || 'text/html';
	$args{'Content-Length'} = int($args{'Content-Length'} || 0);
	$args{'Cache-Control'}  = 'no-cache';
	$args{'Connection'}     = 'close';
	
	my $scode = delete($args{'Scode'});
	my $buff  = '';
	while(my($k,$v) = each(%args)) {
		$buff .= "$k: $v\r\n";
	}
	$buff .= "\r\n";
	
	$self->{super}->Network->WriteDataNow($sock, "HTTP/1.0 $scode NIL\r\n$buff");
}




sub _JSON_GlobalStats {
	my($self) = @_;
	return "({ \"sent\" : \"".$self->{super}->Network->GetStats->{'sent'}."\", \"recv\" : \"".$self->{super}->Network->GetStats->{'recv'}."\" })\n";
}

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

sub _JSON_InfoTorrent {
	my($self, $hash) = @_;
	my %info = ();
	if(my $so = $self->{super}->Storage->OpenStorage($hash)) {
		my $stats = $self->{super}->Queue->GetStats($hash);
		%info = %$stats;
		$info{name}       = $so->GetSetting('name');
		$info{type}       = $so->GetSetting('type');
		$info{paused}     = ($so->GetSetting('_paused') ? 1 : 0);
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

sub _JSON_ShowFiles {
	my($self, $hash) = @_;
	my $r    = $self->{super}->Admin->ExecuteCommand("files", $hash, "list");
	my @list = ();
	foreach my $ar (@{$r->{MSG}}) {
		push(@list, '"'.$self->_sEsc($ar->[1]).'"');
	}
	return '['."\n  ".join(",\n  ",@list)."\n".']'."\n";
}
sub _JSON_ShowPeers {
	my($self, $hash) = @_;
	my $r    = $self->{super}->Admin->ExecuteCommand("peerlist", $hash);
	my @list = ();
	foreach my $ar (@{$r->{MSG}}) {
		push(@list, '"'.$self->_sEsc($ar->[1]).'"') if !$ar->[0];
	}
	return '['."\n  ".join(",\n  ",@list)."\n".']'."\n";
}


sub _sEsc {
	my($self, $str) = @_;
	$str =~ tr/\\//d;
	$str =~ s/"/\\"/gm;
	$str =~ s/&/&amp;/gm;
	$str =~ s/</&lt;/gm;
	$str =~ s/>/&gt;/gm;
	return $str;
}

1;

package Bitflu::AdminHTTP::Data;

	sub new {
		my($class) = @_;
		my $self = {};
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
	
	
	sub _Index {
		my $buff = << 'EOF';
<html>
<head>
<title>Bitflu Web-Gui</title>

<style type="text/css">
	BODY {
		font-family: Helvetica, Arial, sans-serif;
		font-size: 12px;
		background: url("/bg_blue.png");
	}
	
	.pWindow {
		background: url("/bg_lblue.png");
		font-weight:bold;
		cursor : move;
		white-space : nowrap;
	}
	
	.tTable {
		background: url("/bg_white.png");
		padding: 5px;
	}
	
	.dlHeader {
		font-weight: bold;
	}
	
	
	.dlStalled {
		color: #33383dd;
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
		background: url("/bt_white.png");
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
	
</style>

<script language="JavaScript">

var refreshable   = new Array();
var moving_window = 0;
var mouse_off_y   = 0;
var mouse_off_x   = 0;
var mouse_now_y   = 0;
var mouse_now_x   = 0;
var top_z_num     = 0;

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
	content += "<button onClick=\"refreshable['" +key+"']='updateDetailWindow';refreshInterface();\"><b>&lt;</b></button>";
	content += "<button onClick=\"removeDialog('" + key + "')\" ><b>x</b></div>";
	element.innerHTML      = content;
	document.body.appendChild(element);
	refreshable[key] = ""+xfunc;
	refreshInterface();
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

function updateTorrents() {
	var x = new reqObj();
	x.onreadystatechange=function()	{
		if (x.readyState == 4 && x.status == 200) {
			var t_array = eval(x.responseText);
			var t_html  = '<table border="0" width="100%" cellspacing=0 class=tTable>';
			    t_html += "<tr class=dlHeader><td>Name</td><td>Progress</td><td>Done (MB)</td><td>Peers</td><td>Up</td><td>Down</td></tr>";
			for(var i=0; i<t_array.length; i++) {
				var t_obj   = t_array[i];
				var t_id    = t_obj['key'];
				var t_style = 'dlStalled';
				var t_bgcol = '#6688ab';
				var percent = ( (t_obj['done_bytes']+1)/(t_obj['total_bytes']+1)*100).toFixed(1);
				if(t_obj['paused'] == 1)                                { t_style = 'dlPaused';    t_bgcol='#898989'}
				else if(t_obj['committed'] == 1)                        { t_style = 'dlCommitted'; t_bgcol='#447544'}
				else if(t_obj['done_chunks'] == t_obj['total_chunks'] ) { t_style = 'dlComplete';  }
				else if(t_obj['active_clients'] > 0)                    { t_style = 'dlRunning';   }
				else if(t_obj['clients'] == 0)                          { t_style = 'dlDead';      }
				
				t_html += "<tr class="+t_style+" id='item_" + t_id + "' onClick=\"addJsonDialog('updateDetailWindow', '" +t_id+"','loading')\">";
				t_html += "<td>" + t_obj['name'] + "</td>";
				t_html += "<td><div class=pbBorder><div class=pbFiller style=\"background-color:"+t_bgcol+";width: "+percent+"%\"></div></div></td>";
				t_html += "<td>" + (t_obj['done_bytes']/1024/1024).toFixed(1) + "/" + (t_obj['total_bytes']/1024/1024).toFixed(1) + "</td>";
				t_html += "<td>" + t_obj['active_clients'] + "/" + t_obj['clients'] + "</td>";
				t_html += "<td>" + (t_obj['speed_upload']/1024).toFixed(1) + "</td>";
				t_html += "<td>" + (t_obj['speed_download']/1024).toFixed(1) + "</td>";
				t_html += "</tr>";
			}
			t_html += "</table>";
			document.getElementById("tlist").innerHTML = t_html;
			delete x['onreadystatechange'];
			x = null;
		}
	}
	x.open("GET", "/torrentList", true);
	x.send(null);
}

function updateStats() {
	var x = new reqObj();
	x.onreadystatechange=function() {
		if (x.readyState == 4 && x.status == 200) {
			var stats = eval(x.responseText);
			var xup   = stats['sent']/1024;
			var xdown = stats['recv']/1024;
			document.getElementById("stats").innerHTML = "Upload: " + xup.toFixed(2) + " KiB/s / Download: " + xdown.toFixed(2) + " KiB/s";
			delete x['onreadystatechange'];
			x = null;
		}
	}
	x.open("GET", "/stats", true);
	x.send(null);
}

function updateDetailWindow(key) {
	var element = document.getElementById("content_" + key);
	var x = new reqObj();
	x.onreadystatechange=function() {
		if (x.readyState == 4 && x.status == 200) {
			var t_info = eval(x.responseText);
			var t_html = '<table border=1>';
			    t_html += '<tr><td>Name</td><td>' + t_info['name'] + '</td></tr>';
			    t_html += '<tr><td>Network</td><td>' + t_info['type'] + '</td></tr>';
			    t_html += '<tr><td>Downloaded</td><td>' + (t_info['done_bytes']/1024/1024).toFixed(2) + ' MB ('+t_info['done_chunks']+'/'+t_info['total_chunks']+' pieces)</td></tr>';
			    t_html += '<tr><td>Uploaded</td><td>'   + (t_info['uploaded_bytes']/1024/1024).toFixed(2) + 'MB</td></tr>';
			    t_html += '<tr><td>Peers</td><td>' +t_info['clients']+' peers connected, '+t_info['active_clients']+' of them are active</td></tr>';
			    t_html += '<tr><td>Committed</td><td>' + (t_info['committed'] == 1 ? 'Yes' : 'No') + '</td></tr>';
			    t_html += '<tr><td>Commit running</td><td>' + (t_info['committing'] == 1 ? 'Yes: '+t_info['commitinfo'] : 'No') + '</td></tr>';
			    t_html += '</table>';
			    if(t_info['paused'] == 1) {
			       t_html += '<button onclick="_rpcResume(\''+t_info['key']+'\')">Resume</button>';
			    }
			    else {
			       t_html += '<button onclick="_rpcPause(\''+t_info['key']+'\')">Pause</button>';
			    }
			    t_html += '<button onclick="confirmCancel(\''+t_info['key']+'\')">Cancel</button>';
			    t_html += '<button onclick="_rpcShowFiles(\''+t_info['key']+'\')">Show Files</button>';
			    t_html += '<button onclick="_rpcPeerlist(\''+t_info['key']+'\')">Display Peers</button>';
			delete x['onreadystatechange'];
			x = null;
			if(refreshable[key] == 'updateDetailWindow') {
				element.innerHTML = t_html;
				document.getElementById("title_" + key).innerHTML = t_info['name'];
			}
		}
	}
	x.open("GET", "/info/"+key, true);
	x.send(null);
}

function confirmCancel(key) {
	delete refreshable[key]; // This is not refreshable in any way
	var element = document.getElementById("content_" + key);
	var t_html =  "Are you sure?<hr>";
	    t_html += '<button onclick="removeDialog(\''+key+'\')">No</button> ';
	    t_html += '<button onclick="_rpcCancel(\''+key+'\')">Yes, cancel it</button>';
	element.innerHTML = t_html;
}

function _rpcCancel(key) {
	var x = new reqObj();
	x.open("GET", "/cancel/"+key, true);
	x.send(null);
	removeDialog(key);
	refreshInterface(1);
}

function _rpcPause(key) {
	var x = new reqObj();
	x.open("GET", "/pause/"+key, true);
	x.send(null);
	refreshInterface(1);
}

function _rpcResume(key) {
	var x = new reqObj();
	x.open("GET", "/resume/"+key, true);
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
			var t_html = '<table border=1>';
			for(var i=0; i < t_info.length; i++) {
				var tosplit = t_info[i].replace(/\|/g, "</td><td>");
				var t_link  = '';
				if(i > 0) {
					t_link = '<a href=/getfile/'+key+'/'+(i-1)+'>download</a>';
				}
				t_html += "<tr><td>" + tosplit + "</td><td>" + t_link + "</td></tr>\n";
			}
			t_html += "</table>\n";
			delete x['onreadystatechange'];
			x = null;
			if(refreshable[key] == '_rpcShowFiles') {
				element.innerHTML = t_html;
			}
		}
	}
	x.open("GET", "/showfiles/"+key, true);
	x.send(null);
}

function _rpcPeerlist(key) {
	refreshable[key] = '_rpcPeerlist';
	var element = document.getElementById("content_" + key);
	var x = new reqObj();
	x.onreadystatechange=function() {
		if (x.readyState == 4 && x.status == 200) {
			var t_info = eval(x.responseText);
			var t_html = '<table border=0>';
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
	x.open("GET", "/peerlist/"+key, true);
	x.send(null);
}

function refreshInterface(gui) {
	
	if(gui) {
		updateTorrents();
		updateStats();
	}
	for(var i in refreshable) {
		var code = refreshable[i] + "('" + i +"');";
		eval(code);
	}
}

function initInterface() {
	refreshInterface(1);
	setInterval('refreshInterface(1)', 2500);
}

</script>

</head>
<body onLoad="initInterface()" onMouseMove="dragItem(event)" onMouseUp="dragOFF()">

<p id="stats">
<i>Loading statistics...</i>
</p>

<p id="tlist">
<i>Loading download list...</i>
</p>

</body>
</html>
EOF
	return($buff);
	}
	
	
	
1;
