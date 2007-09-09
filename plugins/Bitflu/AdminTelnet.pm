package Bitflu::AdminTelnet;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use constant ANSI_ESC    => "\x1b[";
use constant ANSI_BOLD   => '1;';
use constant ANSI_BLACK  => '30m';
use constant ANSI_RED    => '31m';
use constant ANSI_GREEN  => '32m';
use constant ANSI_YELLOW => '33m';
use constant ANSI_BLUE   => '34m';
use constant ANSI_RSET   => '0m';


use constant KEY_DOWN    => 66;
use constant KEY_UP      => 65;
use constant KEY_TAB     => 9;
use constant KEY_CTRLC   => 3;

use constant PROMPT => '# ';

use constant NOTIFY_BUFF => 20;

##########################################################################
# Register this plugin
sub register {
	my($class,$mainclass) = @_;
	my $self = { super => $mainclass, notifyq => [], notifyi => 0, notifyr => 0, sockbuffs => {} };
	bless($self,$class);
	
	$self->{telnet_port}    = 4001;
	$self->{telnet_bind}    = '127.0.0.1';
	$self->{telnet_maxhist} = ($mainclass->Configuration->GetValue('telnet_maxhist') || 20);
	$mainclass->Configuration->SetValue('telnet_maxhist', $self->{telnet_maxhist});
	
	foreach my $funk qw(telnet_port telnet_bind) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		if(defined($this_value)) {
			$self->{$funk} = $this_value;
		}
		else {
			$mainclass->Configuration->SetValue($funk,$self->{$funk});
		}
		$mainclass->Configuration->RuntimeLockValue($funk);
	}
	
	
	
	my $sock = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$self->{telnet_port}, Bind=>$self->{telnet_bind}, MaxPeers=>5);
	unless($sock) {
		$self->panic("Unable to bind to $self->{telnet_bind}:$self->{telnet_port} : $!");
	}
	
	$self->info(" >> Telnet plugin ready, use 'telnet $self->{telnet_bind} $self->{telnet_port}' to connect.");
	$mainclass->AddRunner($self);
	$mainclass->Admin->RegisterNotify($self, "_Receive_Notify");
	return $self;
}

##########################################################################
# Register  private commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('vd' ,     $self, '_Command_ViewDownloads', 'Display download queue');
	$self->{super}->Admin->RegisterCommand('ls' ,     $self, '_Command_ViewDownloads', 'Display download queue');
	$self->{super}->Admin->RegisterCommand('die',     $self, '_Command_Shutdown'     , 'Terminates bitflu');
	$self->{super}->Admin->RegisterCommand('crashdump', $self, '_Command_Crashdump'  , 'Crashes bitflu (used for debugging)');
	$self->{super}->Admin->RegisterCommand('notify',  $self, '_Command_Notify'       , 'Sends a note to other connected telnet clients');
	return 1;
}


##########################################################################
# Display current downloads
sub _Command_ViewDownloads {
	my($self) = @_;
	my $qlist = $self->{super}->Queue->GetQueueList();
	
	my @buff         = ();
	my $total_peers  = 0;
	my $active_peers = 0;
	push(@buff, [1, "Dummy"]);
	push(@buff, [undef, ">[Type] Name                     /================= Hash =================\\ A/T Peers |\   Pieces  |   Done (MB)   | Done | Ratio| UP    | DOWN | FLAGS"]);
	foreach my $type (sort(keys(%$qlist))) {
		foreach my $key (sort(keys(%{$qlist->{$type}}))) {
			my $padding = " " x 24;
			my $stats   = $self->{super}->Queue->GetStats($key);
			my $xcolor  = 2;
			
			my $flags = undef;
			
			# First flag = Commit status.
			my $so = $self->{super}->Storage->OpenStorage($key);
			$flags .= ( $so->CommitIsRunning ? "." : ($so->GetSetting('committed') ? substr($so->GetSetting('committed'),0,1) : " ") );
			
			
			my $xline = " [".substr($type,0,4)."] ".substr($qlist->{$type}->{$key}->{name}.$padding,0,24)." |$key| ";
			   $xline .= sprintf("%4d/%4d | %4d/%4d | %6.1f/%6.1f", $stats->{active_clients}   , $stats->{clients},
			                                                        $stats->{done_chunks}      , $stats->{total_chunks},
			                                                        $stats->{done_bytes}/1024/1024  , $stats->{total_bytes}/1024/1024);
			   $xline .= sprintf(" | %3d%% | %4.2f |", (($stats->{done_chunks})/($stats->{total_chunks}))*100,
			                                           ($stats->{uploaded_bytes}/(1+$stats->{done_bytes})));
			   $xline .= sprintf(" %5.1f |%5.1f |",       $stats->{speed_upload}/1024, $stats->{speed_download}/1024);
			   $xline .= sprintf(" %s",$flags);
			if($stats->{done_chunks}       == $stats->{total_chunks}) { $xcolor = 3 }
			elsif($stats->{active_clients} > 0 )                      { $xcolor = 1 }
			elsif($stats->{clients}  > 0 )                            { $xcolor = 0 }
			$total_peers  += $stats->{clients};
			$active_peers += $stats->{active_clients};
			push(@buff, [$xcolor, $xline]);
		}
	}
	$buff[0] = [1, sprintf(" *** Upload: %6.2f KiB/s | Download: %6.2f KiB/s | Peers: %3d/%3d",
	                ($self->{super}->Network->GetStats->{'sent'}/1024),
	                ($self->{super}->Network->GetStats->{'recv'}/1024),
	                $active_peers, $total_peers)];


	return {CHAINSTOP=>1, MSG=>\@buff};
}

##########################################################################
# bye!
sub _Command_Shutdown {
	my($self) = @_;
	kill(2,$$);
	return {CHAINSTOP=>1, MSG=>[ [1, "Shutting down $0 (with pid $$)"] ]};
}

sub _Command_Crashdump {
	my($self) = @_;
	$self->info("Crashdumping: ".Carp::cluck());
	exit(1);
}

##########################################################################
# Send out a notification
sub _Command_Notify {
	my($self, @args) = @_;
	my $string = join(' ', @args);
	$self->{super}->Admin->SendNotify($string);
	return {CHAINSTOP=>1, MSG=>[ [ 1, 'notification sent'] ]};
}

##########################################################################
# Receive a notification (Called via Admin)
sub _Receive_Notify {
	my($self, $string) = @_;
	my $numi   = ++$self->{notifyi};
	my $numnot = push(@{$self->{notifyq}}, {id=>$numi,msg=>$string});
	shift(@{$self->{notifyq}}) if $numnot > NOTIFY_BUFF;
	$self->debug("Notification with ID $numi received ($numnot notifications buffered)");
}


##########################################################################
# Own runner command
sub run {
	my($self) = @_;
	
	if($self->{notifyr} != $self->{super}->Network->GetTime) {
		$self->{notifyr} = $self->{super}->Network->GetTime;
		foreach my $csock (keys(%{$self->{sockbuffs}})) {
			my $tsb = $self->{sockbuffs}->{$csock};
			next if $tsb->{lastnotify} == $self->{notifyi}; # Does not need notification
			foreach my $notify (@{$self->{notifyq}}) {
				next if $notify->{id} <= $tsb->{lastnotify};
				$tsb->{lastnotify} = $notify->{id};
				$self->{super}->Network->WriteData($csock, "\r".Bold("> [Notification]: $notify->{msg}")."\r\n".PROMPT."$tsb->{cbuff}");
			}
		}
	}
	
	
	$self->{super}->Network->Run($self, {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
}


##########################################################################
# Accept new incoming connection
sub _Network_Accept {
	my($self,$sock) = @_;
	$self->info("New incoming connection from <$sock>");
	$self->panic("Duplicate sockid?!") if defined($self->{sockbuffs}->{$sock});
	# DO = 253 ; DO_NOT = 254 ; WILL = 251 ; WILL NOT 252
	
	my $initcode =  chr(0xff).chr(251).chr(1).chr(0xff).chr(251).chr(3);
	   $initcode .= chr(0xff).chr(254).chr(0x22);
	my $motd     = "# Welcome to Bitflu\r\n".PROMPT;
	$self->{super}->Network->WriteData($sock, $initcode.$motd);
	$self->{sockbuffs}->{$sock} = { cbuff => '', history => [], h => 0, lastnotify => $self->{notifyi} };
}

##########################################################################
# Read data from network
sub _Network_Data {
	my($self,$sock,$buffref) = @_;
	
	my $new_data = ${$buffref};
	my $sb       = $self->{sockbuffs}->{$sock};
	my @exe      = ();
	
	foreach my $c (split(//,$new_data)) {
		my $nc       = ord($c);
		if($sb->{cmd})         { $sb->{cmd}--; }
		elsif($sb->{nav})      {
			$sb->{nav}--;
			my $hist_top = int(@{$sb->{history}})-1;
			next if $sb->{nav};
			next if $hist_top < 0; # Empty history
			
			if($nc == KEY_UP) {
				if($sb->{h} >= 0 && $sb->{h} < $hist_top) {
					$sb->{h}++;
				}
			}
			elsif($nc == KEY_DOWN) {
				if($sb->{h} > 0) {
					$sb->{h}--;
				}
			}
			if($sb->{h} < 0) {
				$sb->{h} = 0;
			}
			my $hindx = $hist_top - $sb->{h};
			push(@exe, ['r', $sb->{history}->[$hindx]]);
		}
		elsif(ord($c) eq 27)   { $sb->{nav} = 2; }
		elsif(ord($c) eq 0xff) { $sb->{cmd} = 2; }
		elsif(ord($c) eq 0x00) { }
		elsif(ord($c) eq 127 or ord($c) eq 126) {
			# -> 'd'elete char (backspace)
			push(@exe, ['d', 1]);
		}
		elsif($c eq "\r") {
			# -> E'X'ecute
			push(@exe, ['X','']);
			last; # Throws away data :-( .. fixme
		}
		elsif($nc == KEY_TAB) {
			my ($sha_part) = $sb->{cbuff} =~ / ([0-9A-Za-z]+)$/;
			next unless defined($sha_part);
			my $ql = $self->{super}->Queue->GetQueueList;
			
			my $tab_numhits = 0;
			my $tab_hit     = undef;
			foreach my $t (keys(%$ql)) {
				foreach my $this_sha (keys(%{$ql->{$t}})) {
					if($this_sha =~ /^$sha_part(.*)$/) {
						$tab_numhits++;
						$tab_hit = $1." ";
					}
				}
			}
			
			if($tab_numhits == 1) {
				push(@exe, ['a', $tab_hit]);
			}
		}
		elsif($nc == KEY_CTRLC) {
			push(@exe, ['C', '']);
		}
		else {
			# 'a'ppend normal char
			$sb->{h} = -1;
			push(@exe, ['a', $c]);
		}
	}
	
	
	foreach my $ocode (@exe) {
		my $tx = undef;
		if($ocode->[0] eq 'a') {
			$self->{sockbuffs}->{$sock}->{cbuff} .= $ocode->[1];
			$tx = $ocode->[1];
		}
		elsif($ocode->[0] eq 'd') {
			$tx = "\r";
			$tx .= PROMPT.(" " x length($sb->{cbuff}))."\r";
			$sb->{cbuff} = substr($sb->{cbuff},0,-1*($ocode->[1]));
			$tx .= PROMPT.$sb->{cbuff};
		}
		elsif($ocode->[0] eq 'r') {
			push(@exe, ['d', length($sb->{cbuff})]);
			push(@exe, ['a', $ocode->[1]]);
		}
		elsif($ocode->[0] eq 'X') {
			my $cmdout = $self->Xexecute($sock, $sb->{cbuff});
			if(!defined($cmdout)) {
				return undef; # q;
			}
			elsif(length($cmdout) != 0) {
				$sb->{h} = -1;
				push (@{$sb->{history}}, $sb->{cbuff});
				shift(@{$sb->{history}}) if int(@{$sb->{history}}) > $self->{telnet_maxhist};
			}
			push(@exe, ['C',$cmdout]);
		}
		elsif($ocode->[0] eq 'C') {
			$sb->{cbuff} = '';
			$tx = "\r\n".$ocode->[1].PROMPT;
		}
		else {
			$self->panic("Unknown opcode '$ocode->[0]'");
		}
		$self->{super}->Network->WriteData($sock, $tx) if defined($tx);
	}
	
}


sub Xexecute {
	my($self, $sock, $cmdstring) = @_;
	
	my($command, @args) = _deToken($cmdstring);
	
	if(!defined($command)) {
		return '';
	}
	elsif($command eq "q" or $command eq "quit") {
		$self->_Network_Close($sock);
		$self->{super}->Network->RemoveSocket($self,$sock);
		return undef;
	}
	elsif(defined($command)) {
		my $exe = $self->{super}->Admin->ExecuteCommand($command,@args);
		my $tb  = '';
		foreach my $alin (@{$exe->{MSG}}) {
			my $cc = ($alin->[0] or 0);
			my $cv = $alin->[1];
			if($exe->{CHAINSTOP} == 0)  { $tb .= Red($cv)    }
			elsif($cc == 1)             { $tb .= Green($cv)  }
			elsif($cc == 2)             { $tb .= Red($cv)    }
			elsif($cc == 3)             { $tb .= Yellow($cv) }
			elsif($cc == 4)             { $tb .= Blue($cv)   }
			else                        { $tb .= $cv;        }
			$tb .= "\r\n";
		}
		return $tb;
	}
	else {
		$self->panic("NOT REACHED");
	}
}

##########################################################################
# Close down TCP connection
sub _Network_Close {
	my($self,$sock) =  @_;
	$self->info("Closing connection with <$sock>");
	delete($self->{sockbuffs}->{$sock});
}

##########################################################################
# Parse tokens
sub _deToken {
	my($line) = @_;
	my @p = ();
	my $aesc = undef;
	my $buff = undef;
	foreach my $token (split(/(\s|\\\"|")/,$line)) {
		next if $token eq '';
		if(defined($aesc)) {
			if($token eq '"') {
				push(@p,_toDE($aesc));
				$aesc=undef;
			}
			else {
				$aesc .= $token;
			}
			next;
		}
		elsif($token eq '"' && !defined($aesc)) {
			# Start escaping
			$aesc = '';
			$aesc .= $buff;
			$buff = undef;
			next;
		}
		elsif($token eq " ") {
			push(@p,_toDE($buff)) if defined $buff;
			$buff = undef;
			next; #Skip command seperation
		}
		$buff .= $token;
	}
	push(@p,_toDE($buff)) if defined $buff;
	push(@p,_toDE($aesc)) if defined $aesc;
	return @p;
}

sub _toDE {
	my($string) = @_;
	$string =~ s/\\\"/"/g;
	return $string;
}


sub Green {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_GREEN.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Blue {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_BLUE.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Yellow {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_YELLOW.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return Bold($s);
}

sub Red {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_RED.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Bold {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC."40m".ANSI_ESC.ANSI_BOLD.ANSI_YELLOW.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub AnsiCure {
	my($s) = @_;
	my $badend = chop($s);
	if($badend ne "\n") { $s .= $badend ; $badend = undef }
	return($s,$badend);
}

sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self)."[Telnet]: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self)." [Telnet]: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self)."[Telnet]: ".$msg); }



1;
