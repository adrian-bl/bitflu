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
use constant ANSI_CYAN   => '35m';
use constant ANSI_WHITE  => '37m';
use constant ANSI_RSET   => '0m';


use constant KEY_DOWN    => 66;
use constant KEY_UP      => 65;
use constant KEY_TAB     => 9;
use constant KEY_CTRLC   => 3;

use constant PROMPT => 'bitflu> ';

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
	
	
	
	my $sock = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$self->{telnet_port}, Bind=>$self->{telnet_bind},
	                                             MaxPeers=>5, Callbacks =>  {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	unless($sock) {
		$self->stop("Unable to bind to $self->{telnet_bind}:$self->{telnet_port} : $!");
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
	$self->{super}->Admin->RegisterCommand('notify',  $self, '_Command_Notify'       , 'Sends a note to other connected telnet clients');
	$self->{super}->Admin->RegisterCommand('details', $self, '_Command_Details'      , 'Display verbose information about given queue_id');
	return 1;
}


##########################################################################
# Display details
sub _Command_Details {
	my($self, @args) = @_;
	
	my @A = ();
	
	if($args[0]) {
		foreach my $sha1 (@args) {
			if(my $so        = $self->{super}->Storage->OpenStorage($sha1)) {
				my $stats      = $self->{super}->Queue->GetStats($sha1);
				my @flayout    = split(/\n/,$so->GetSetting('filelayout'));
				push(@A, [6, "Details for $sha1"]);
				push(@A, [6, ("-" x 52)]);
				push(@A, [0, sprintf("Name                   : %s",      $so->GetSetting('name'))]);
				push(@A, [0, sprintf("Total files            : %d", int(@flayout) )]);
				push(@A, [0, sprintf("Total size             : %.2f MB / %d piece(s)",       $stats->{total_bytes}/1024/1024,$stats->{total_chunks})]);
				push(@A, [0, sprintf("Completed              : %.2f MB / %d piece(s)",       $stats->{done_bytes}/1024/1024, $stats->{done_chunks})]);
				push(@A, [0, sprintf("Uploaded               : %.2f MB",                     $stats->{uploaded_bytes}/1024/1024)]);
				push(@A, [0, sprintf("Peers                  : Connected: %d / Active: %d",  $stats->{clients}, $stats->{active_clients})]);
				push(@A, [0, sprintf("Downloading since      : %s", ($so->GetSetting('createdat') ? "".gmtime($so->GetSetting('createdat')) : 'Unknown'))]);
				push(@A, [0, sprintf("Last piece received at : %s", ($so->GetSetting('_last_recv') ? "".gmtime($so->GetSetting('_last_recv')) : '-'))]);
				push(@A, [0, sprintf("Fully downloaded       : %s", ($stats->{done_chunks} == $stats->{total_chunks} ? "Yes" : "No"))]);
				push(@A, [0, sprintf("Download committed     : %s", ($so->CommitFullyDone ? 'Yes' : 'No'))]);
			}
			else {
				push(@A, [2, "$sha1: does not exist in queue"]);
			}
		}
	}
	else {
		push(@A, [2, "Usage: details queue_id [queue_id2 ...]"]);
	}
	return({CHAINSTOP=>1, MSG=>\@A});
}

##########################################################################
# Display current downloads
sub _Command_ViewDownloads {
	my($self) = @_;
	
	my @a            = ([1, "Dummy"]);
	my $qlist        = $self->{super}->Queue->GetQueueList;
	my $active_peers = 0;
	my $total_peers  = 0;
	
	push(@a, [undef, sprintf(">%6s %-24s %42s %-5s | %-10s | %-14s | %4s | %5s| %4s | %4s |",
	                         '[Type]', 'Name', '/================= Hash =================\\', 'Peers', '  Pieces',
	                         '  Done (MB)', 'Done', 'Ratio', 'Up ', 'Down')]);
	
	foreach my $dl_type (sort(keys(%$qlist))) {
		foreach my $key (sort(keys(%{$qlist->{$dl_type}}))) {
			my $this_stats = $self->{super}->Queue->GetStats($key)      or $self->panic("$key has no stats!");                 # stats for this download
			my $this_so    = $self->{super}->Storage->OpenStorage($key) or $self->panic("Unable to open storage of $key");     # storage-object for this download
			my $this_name  = substr($this_so->GetSetting('name').(" " x 24), 0, 24);                                           # 'gui-save' name
			my $xcolor     = 2;                                                                                                # default is red
			
			my $this_xmsg  = '';
			my $this_sline = sprintf(" [%4s] %-24s |%40s|%3d/%2d |%5d/%5d |%7.1f/%7.1f | %3d%% | %4.2f |%5.1f |%5.1f | ",
			                           $dl_type, $this_name, $key, $this_stats->{active_clients},$this_stats->{clients}, $this_stats->{done_chunks},
			                           $this_stats->{total_chunks}, ($this_stats->{done_bytes}/1024/1024), ($this_stats->{total_bytes}/1024/1024),
			                           (($this_stats->{done_chunks}/$this_stats->{total_chunks})*100), ($this_stats->{uploaded_bytes}/(1+$this_stats->{done_bytes})),
			                           $this_stats->{speed_upload}/1024, $this_stats->{speed_download}/1024,
			);
			
			
			if(my $ci = $this_so->CommitIsRunning) { $this_xmsg .= "Committing file $ci->{file}/$ci->{total_files}, ".int(($ci->{total_size}-$ci->{written})/1024/1024)." MB left"; }
			if($this_so->CommitFullyDone)                                    { $xcolor = 3 }
			elsif($this_stats->{done_chunks} == $this_stats->{total_chunks}) { $xcolor = 4 }
			elsif($this_stats->{active_clients} > 0 )                        { $xcolor = 1 }
			elsif($this_stats->{clients} > 0 )                               { $xcolor = 0 }
			$active_peers += $this_stats->{active_clients};
			$total_peers  += $this_stats->{clients};
			
			push(@a, [$xcolor, $this_sline.$this_xmsg]);
		}
	}
	
	$a[0] = [1, sprintf(" *** Upload: %6.2f KiB/s | Download: %6.2f KiB/s | Peers: %3d/%3d",
	                     ($self->{super}->Network->GetStats->{'sent'}/1024),
	                     ($self->{super}->Network->GetStats->{'recv'}/1024),
	                      $active_peers, $total_peers) ];
	return {CHAINSTOP=>1, MSG=>\@a};
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
				if($tsb->{auth}) {
					my $cbuff = $tsb->{p}.$tsb->{cbuff};
					
					#$self->{super}->Network->WriteDataNow($tsb->{socket}, "\r\n".Alert("> ".localtime()." [Notification]: $notify->{msg}")."\r\n".$tsb->{p}."$tsb->{cbuff}");
					$self->{super}->Network->WriteDataNow($tsb->{socket}, "\r".(" " x length($cbuff))."\r");
					$self->{super}->Network->WriteDataNow($tsb->{socket}, Alert(">".localtime()." [Notification]: $notify->{msg}")."\r\n$cbuff");
				}
			}
		}
	}
	
	
	$self->{super}->Network->Run($self);
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
	$self->{sockbuffs}->{$sock} = { cbuff => '', history => [], h => 0, lastnotify => $self->{notifyi}, p => PROMPT, echo => 1, socket => $sock,
	                                auth => $self->{super}->Admin->AuthenticateUser(User=>'', Pass=>''), auth_user=>undef };
	
	$self->{sockbuffs}->{$sock}->{p} = 'Login: ' unless $self->{sockbuffs}->{$sock}->{auth};
	
	my $motd     = "# Welcome to Bitflu\r\n".$self->{sockbuffs}->{$sock}->{p};
	$self->{super}->Network->WriteDataNow($sock, $initcode.$motd);
}

##########################################################################
# Read data from network
sub _Network_Data {
	my($self,$sock,$buffref) = @_;
	
	my $new_data = ${$buffref};
	my $sb       = $self->{sockbuffs}->{$sock};
	my @exe      = ();
	
	my $piggy    = '';
	my $cseen    = 0;
	foreach my $c (split(//,$new_data)) {
		my $nc       = ord($c);
		   $cseen   += 1;
		
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
		elsif($c eq "\n")      { }
		elsif(ord($c) eq 127 or ord($c) eq 126) {
			# -> 'd'elete char (backspace)
			push(@exe, ['d', 1]);
		}
		elsif($c eq "\r") {
			# -> E'X'ecute
			if($sb->{auth}) {
				push(@exe, ['X',undef]);
			}
			else {
				push(@exe, ['C','']);
				
				if(!defined($sb->{auth_user})) { $sb->{auth_user} = $sb->{cbuff}; $sb->{p} = "Password: "; $sb->{echo} = 0; }
				elsif($self->{super}->Admin->AuthenticateUser(User=>$sb->{auth_user}, Pass=>$sb->{cbuff})) {
					$sb->{auth} = 1;
					$sb->{p}    = $sb->{auth_user}.'@'.PROMPT;
					$sb->{echo} = 1;
					$self->info("Telnet login for user $sb->{auth_user} completed");
				}
				else {
					$self->info("Telnet login for user $sb->{auth_user} failed");
					push(@exe, ['X','quit']);
				}
			}
			$piggy = substr($new_data,$cseen); # Save piggyback data
			last;
		}
		elsif($nc == KEY_TAB) {
			my($cmd_part)  = $sb->{cbuff} =~ /^(\S+)$/;
			my($sha_part)  = $sb->{cbuff} =~ / ([0-9A-Za-z]*)$/;
			
			my $searchlist = undef;
			my $searchstng = undef;
			
			if(defined($cmd_part)) {
				$searchlist = $self->{super}->Admin->GetCommands;
				$searchstng = $cmd_part;
			}
			elsif(defined($sha_part)) {
				my $queuelist = $self->{super}->Queue->GetQueueList;
				foreach my $qt (keys(%$queuelist)) {
					foreach my $qi (keys(%{$queuelist->{$qt}})) {
						$searchlist->{$qi} = $qi;
					}
				}
				$searchstng = $sha_part;
			}
			
			if(defined($searchlist)) {
				my $num_hits = 0;
				my $str_hit  = '';
				foreach my $t (keys(%$searchlist)) {
					if($t =~ /^$searchstng(.+)$/) {
						$num_hits++;
						$str_hit = $1.' ';
					}
				}
				
				if($num_hits == 1 && $sb->{auth}) { # Exact match AND connection is authenticated (= Can see 'secret' data)
					push(@exe, ['a', $str_hit]);
				}
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
			$tx = $ocode->[1] if $sb->{echo};
		}
		elsif($ocode->[0] eq 'd') {
			$tx = "\r";
			$tx .= $sb->{p}.(" " x length($sb->{cbuff}))."\r";
			$sb->{cbuff} = substr($sb->{cbuff},0,-1*($ocode->[1]));
			$tx .= $sb->{p}.$sb->{cbuff};
		}
		elsif($ocode->[0] eq 'r') {
			push(@exe, ['d', length($sb->{cbuff})]);
			push(@exe, ['a', $ocode->[1]]);
		}
		elsif($ocode->[0] eq 'X') {
			my $cmdout = $self->Xexecute($sock, (defined($ocode->[1]) ? $ocode->[1] : $sb->{cbuff}));
			if(!defined($cmdout)) {
				return undef; # quit;
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
			$tx = "\r\n".$ocode->[1].$sb->{p};
		}
		else {
			$self->panic("Unknown opcode '$ocode->[0]'");
		}
		$self->{super}->Network->WriteDataNow($sock, $tx) if defined($tx);
	}
	
	if(length($piggy) != 0) {
		$self->_Network_Data($sock,\$piggy);
	}
	
}


sub Xexecute {
	my($self, $sock, $cmdstring) = @_;
	
	my($command, @args) = _deToken($cmdstring);
	if(!defined($command)) {
		return '';
	}
	elsif($command eq "q" or $command eq "quit" or $command eq "exit" or $command eq "logout") {
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
			elsif($cc == 4)             { $tb .= Cyan($cv)   }
			elsif($cc == 5)             { $tb .= Blue($cv)  }
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

sub Cyan {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_CYAN.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Yellow {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_YELLOW.BgBlack($string).ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Blue {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_BLUE.BgBlack($string).ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Red {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_RED.$string.ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub Alert {
	my($s) = @_;
	my ($string,$end) = AnsiCure($s);
	$s = ANSI_ESC.ANSI_BOLD.ANSI_WHITE.BgBlue($s).ANSI_ESC.ANSI_RSET;
	$s .= $end if defined($end);
	return $s;
}

sub BgBlue {
	my($s) = @_;
	$s = ANSI_ESC."44m".$s;
	return $s;
}

sub BgBlack {
	my($s) = @_;
	$s = ANSI_ESC."40m".$s;
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
sub stop { my($self, $msg) = @_; $self->{super}->stop(ref($self)."[Telnet]: ".$msg); }



1;
