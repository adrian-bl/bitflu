package Bitflu::AdminHTTP;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2008 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;

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
# Own runner command
sub run {
	my($self) = @_;
	$self->{super}->Network->Run($self);
	
	foreach my $socknam ($self->GetSockets) {
		my $sockstat = $self->GetSockState($socknam);
		my $sockglob = $self->{sockets}->{$socknam}->{socket};
		if($sockstat == STATE_SENDBODY && $self->{super}->Network->GetQueueLen($sockglob) == 0) {
			$self->DropConnection($sockglob);
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
	
	if($self->IsAuthenticated($sock)) {
		
	}
	
	print "REQUEST: $rq->{GET}\n";
	
	if($rq->{GET} eq '/torrentList') {
		$data = $self->_JSON_TorrentList;
	}
	elsif($rq->{GET} eq '/stats') {
		$data = $self->_JSON_GlobalStats;
	}
	elsif($rq->{GET} =~ /^\/info\/([a-z0-9]{40})$/) {
		$data = $self->_JSON_InfoTorrent($1);
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
	$self->{sockets}->{$sock} = { buffer => '', bufflen => 0, socket=>$sock, state=>STATE_READHEADER,
	                              timeout_at => $self->{super}->Network->GetTime+SOCKET_TIMEOUT };
	$self->warn("Registered HTTP-Sock <$sock>");
}

##########################################################################
# Remove a socket
sub RemoveSocket {
	my($self, $sock) = @_;
	delete($self->{sockets}->{$sock}) or $self->panic("Unable to remove <$sock>, did not exist?!");
	$self->warn("Removed HTTP-Sock <$sock>");
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
		$self->warn("Added $len bytes to <$sock> buff");
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
# Returns 1 if socket is authenticated
sub IsAuthenticated {
	my($self,$sock,$state) = @_;
	my $sr = $self->{sockets}->{$sock} or $self->panic("$sock does not exist");
	return 1;
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
	$args{'Content-Length'} = length($args{Payload});
	$self->_HttpSendOk($sock, %args);
	$self->{super}->Network->WriteDataNow($sock, $args{Payload});
}


sub _HttpSendOk {
	my($self, $sock, %args) = @_;
	my $ctype = $args{'Content-Type'}   || 'text/html';
	my $clen  = int($args{'Content-Length'} || 0);
	$self->{super}->Network->WriteDataNow($sock, "HTTP/1.0 200 OK\r\nContent-Type: $ctype\r\nContent-Length: $clen\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n");
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
			my $this_so    = $self->{super}->Storage->OpenStorage($key) or $self->panic("Unable to open storage of $key");
			my $this_stats = $self->{super}->Queue->GetStats($key)      or $self->panic("$key has no stats!");
			my $this_name  = $this_so->GetSetting('name');
			   $this_name =~ tr/A-Za-z0-9\. _-//cd;
			
			my %info = %$this_stats;
			   $info{name} = $this_name;
			   $info{type} = $dl_type;
			   $info{key}  = $key;
			my $json = "{ ";
			while(my($k,$v) = each(%info)) {
				$json .= "\"$k\" : \"$v\",";
			}
			chop($json);
			$json .= " }";
			push(@list, $json);
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
		$info{name} = $so->GetSetting('name');
		$info{type} = $so->GetSetting('type');
	}
	$info{key} = $hash;
	my $json = "({ ";
	while(my($k,$v) = each(%info)) {
		$json .= "\"$k\" : \"$v\",";
	}
	chop($json);
	$json .= " })\n";
	return $json;
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
<title>Test</title>

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
	}
	
	.tTable {
		background: url("/bg_white.png");
		padding: 4px;
	}
	
	.tTable tr:hover {
		background: url("/bg_lblue.png");
	}
</style>

<script language="JavaScript">

var moving_window = 0;
var mouse_off_y   = 0;
var mouse_off_x   = 0;
var mouse_now_y   = 0;
var mouse_now_x   = 0;


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


function removeDialog(id) {
	document.body.removeChild(document.getElementById("window_" + id));
}

function addDetails(key) {
	if(document.getElementById("window_"+key)) {
		return false;
	}
	
	var element = document.createElement('div');
	var content = '';
	element.id             = "window_"+key;
	element.className      = 'tTable';
	element.style.top      = mouse_now_y;
	element.style.left     = mouse_now_x;
	element.style.position = 'absolute';
	element.style.border   = '2px solid #001100';
	
	content += "<div class=pWindow OnMouseDown=\"dragON('"+key+"')\">" + key + " <a onClick=\"removeDialog('" + key + "')\"><b>X</b></a>";
	content += "</div>\n";
	content += "<p id=\"content_"+key+"\">...</p>";
	
	element.innerHTML      = content;
	document.body.appendChild(element);
	updateDetailWindow(key);
}

function dragON(key) {
	moving_window = key;
	var element   = document.getElementById("window_" + moving_window);
	var moving_at_x   = parseInt(element.style.left);
	var moving_at_y   = parseInt(element.style.top);
	mouse_off_x = mouse_now_x - moving_at_x;
	mouse_off_y = mouse_now_y - moving_at_y;
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
			var t_html  = '<table border="0" width="100%" background="/bg_white.png" cellspacing=0>';
			    t_html += "<tr><td>Name</td><td>Peers</td><td>Done (MB)</td><td>Up</td><td>Down</td></tr>";
			for(var i=0; i<t_array.length; i++) {
				var t_obj = t_array[i];
				var t_id  = t_obj['key'];
				t_html += "<tr id='item_" + t_id + "' onClick=addDetails('" +t_id+"')>";
				t_html += "<td>" + t_obj['name'] + "</td><td>" + t_obj['active_clients'] + "/" + t_obj['clients'] + "</td>";
				t_html += "<td>" + (t_obj['done_bytes']/1024/1024).toFixed(1) + "/" + (t_obj['total_bytes']/1024/1024).toFixed(1) + "</td>";
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
			    t_html += '<tr><td>Downloaded</td><td>' + (t_info['done_bytes']/1024/1024).toFixed(2) + ' MB ('+t_info['done_chunks']+'/'+t_info['total_chunks']+' pieces)</td></tr>';
			    t_html += '<tr><td>Uploaded</td><td>'   + (t_info['uploaded_bytes']/1024/1024).toFixed(2) + 'MB</td></tr>';
			    t_html += '<tr><td>Name</td><td>' + t_info['name'] + '</td></tr>';
			    t_html += '</table>';
			    t_html += '<button>Pause</button> <button>Cancel</button>';
			element.innerHTML = t_html;
			delete x['onreadystatechange'];
			x = null;
		}
	}
	x.open("GET", "/info/"+key, true);
	x.send(null);
}

function refreshInterface() {
	updateTorrents();
	updateStats();
}

function initInterface() {
	refreshInterface();
	setInterval('refreshInterface()', 2000);
}

</script>

</head>
<body onLoad="initInterface()" onMouseMove="dragItem(event)" onMouseUp="dragOFF()">

<p id="stats">
<i>Loading statistics...</i>
</p>

<p id="tlist" class="tTable">
<i>Loading download list...</i>
</p>

</body>
</html>
EOF
	return($buff);
	}
	
	
	
1;
