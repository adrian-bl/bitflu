package Bitflu::AdminTelnet;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2011 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#

use strict;
use POSIX qw(ceil);
use Encode;
use constant _BITFLU_APIVERSION => 20110912;

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


use constant KEY_C_LEFT  => 100;
use constant KEY_C_RIGHT => 99;
use constant KEY_LEFT    => 68;
use constant KEY_RIGHT   => 67;
use constant KEY_DOWN    => 66;
use constant KEY_UP      => 65;
use constant KEY_TAB     => 9;
use constant KEY_CTRLA   => 1;
use constant KEY_CTRLC   => 3;
use constant KEY_CTRLD   => 4;
use constant KEY_CTRLE   => 5;
use constant KEY_CTRLL   => 12;

use constant PROMPT => 'bitflu> ';

use constant NOTIFY_BUFF => 20;

##########################################################################
# Register this plugin
sub register {
	my($class,$mainclass) = @_;
	my $self = { super => $mainclass, notifyq => [], notifyi => 0, sockbuffs => {} };
	bless($self,$class);
	
	
	my $xconf = { telnet_port=>4001, telnet_bind=>'127.0.0.1', telnet_maxhist=>20 };
	my $lock  = { telnet_port=>1,    telnet_bind=>1                               };
	foreach my $funk (keys(%$xconf)) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		
		if(defined($this_value)) {
			$xconf->{$funk} = $this_value;
		}
		else {
			$mainclass->Configuration->SetValue($funk,$xconf->{$funk});
		}
		$mainclass->Configuration->RuntimeLockValue($funk) if $lock->{$funk};
	}
	
	
	
	my $sock = $mainclass->Network->NewTcpListen(ID=>$self, Port=>$xconf->{telnet_port}, Bind=>$xconf->{telnet_bind}, DownThrottle=>0,
	                                             MaxPeers=>5, Callbacks =>  {Accept=>'_Network_Accept', Data=>'_Network_Data', Close=>'_Network_Close'});
	unless($sock) {
		$self->stop("Unable to bind to $xconf->{telnet_bind}:$xconf->{telnet_port} : $!");
	}
	
	$self->info(" >> Telnet plugin ready, use 'telnet $xconf->{telnet_bind} $xconf->{telnet_port}' to connect.");
	$mainclass->AddRunner($self);
	$mainclass->Admin->RegisterNotify($self, "_Receive_Notify");
	return $self;
}

##########################################################################
# Register  private commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('vd' ,       $self, '_Command_ViewDownloads', 'Display download queue');
	$self->{super}->Admin->RegisterCommand('ls' ,       $self, '_Command_ViewDownloads', 'Display download queue');
	$self->{super}->Admin->RegisterCommand('list' ,     $self, '_Command_ViewDownloads', 'Display download queue');
	$self->{super}->Admin->RegisterCommand('notify',    $self, '_Command_Notify'              , 'Sends a note to other connected telnet clients');
	$self->{super}->Admin->RegisterCommand('details',   $self, '_Command_Details'             , 'Display verbose information about given queue_id');
	$self->{super}->Admin->RegisterCommand('crashdump', $self, '_Command_CrashDump'           , 'Crashes bitflu');
	$self->{super}->Admin->RegisterCommand('quit',      $self, '_Command_BuiltinQuit'         , 'Disconnects current telnet session');
	$self->{super}->Admin->RegisterCommand('grep',      $self, '_Command_BuiltinGrep'         , 'Searches for given regexp');
	$self->{super}->Admin->RegisterCommand('sort',      $self, '_Command_BuiltinSort'         , 'Sort output. Use "sort -r" for reversed sorting');
	$self->{super}->Admin->RegisterCommand('head',      $self, '_Command_BuiltinHead'         , 'Print the first 10 lines of input');
	$self->{super}->Admin->RegisterCommand('tail',      $self, '_Command_BuiltinTail'         , 'Print the last 10 lines of input');
	$self->{super}->Admin->RegisterCommand('repeat',    $self, '_Command_BuiltinRepeat'       , 'Executes a command each second');
	$self->{super}->Admin->RegisterCommand('clear',     $self, '_Command_Clear'               , 'Clear telnet screen');
	$self->{super}->Admin->RegisterCompletion($self, '_Completion');
	return 1;
}


##########################################################################
# Returns a suggestion list
sub _Completion {
	my($self,$hint) = @_;
	
	my @list   = ();
	
	if($hint eq 'cmd') {
		@list = keys(%{$self->{super}->Admin->GetCommands});
	}
	else {
		my $ql     = $self->{super}->Queue->GetQueueList;
		foreach my $qt (keys(%$ql)) {
			foreach my $qi (keys(%{$ql->{$qt}})) {
				push(@list,$qi);
			}
		}
	}
	return @list;
}

##########################################################################
# This should never get called.. but if someone dares to do it...
sub _Command_BuiltinQuit {
	my($self) = @_;
	return({MSG=>[[2, "This is a builtin command of ".__PACKAGE__]], SCRAP=>[]});
}

##########################################################################
# Non-Catched (= Unpiped) grep command
sub _Command_BuiltinGrep {
	my($self) = @_;
	return({MSG=>[[2, "grep must be used after a pipe. Example: help | grep [-v] peer"]], SCRAP=>[]});
}

##########################################################################
# Non-Catched (= Unpiped) sort command
sub _Command_BuiltinSort {
	my($self) = @_;
	return({MSG=>[[2, "tail must be used after a pipe. Example: help | tail"]], SCRAP=>[]});
}

##########################################################################
# Non-Catched (= Unpiped) sort command
sub _Command_BuiltinHead {
	my($self) = @_;
	return({MSG=>[[2, "head must be used after a pipe. Example: help | head"]], SCRAP=>[]});
}

##########################################################################
# Non-Catched (= Unpiped) sort command
sub _Command_BuiltinTail {
	my($self) = @_;
	return({MSG=>[[2, "sort must be used after a pipe. Example: help | sort"]], SCRAP=>[]});
}

##########################################################################
# Clear screen
sub _Command_Clear {
	my($self) = @_;
	return({MSG=>[[0xff, '']], SCRAP=>[]});
}

##########################################################################
# Non-Catched (= Unpiped) repeat command
sub _Command_BuiltinRepeat {
	my($self) = @_;
	return({MSG=>[[2, "repeat requires an argument. Example: repeat clear ; date ; vd"]], SCRAP=>[]});
}

sub _Command_CrashDump  {
	my($self) = @_;
	
	open(X, ">", "./workdir/tmp/crash.dump.$$") or die;
	print X Data::Dumper::Dumper($self);
	close(X);
	$self->panic("Whee");
}


##########################################################################
# Display details
sub _Command_Details {
	my($self, @args) = @_;
	
	my @MSG    = ();
	my @SCRAP  = ();
	my $NOEXEC = '';
	$self->{super}->Tools->GetOpts(\@args);
	
	if($args[0]) {
		foreach my $sha1 (@args) {
			if(my $so        = $self->{super}->Storage->OpenStorage($sha1)) {
				my $stats      = $self->{super}->Queue->GetStats($sha1);
				push(@MSG, [6, "Details for $sha1"]);
				push(@MSG, [6, ("-" x 52)]);
				push(@MSG, [0, sprintf("Name                   : %s", $so->GetSetting('name'))]);
				push(@MSG, [0, sprintf("Download hash          : %s", "sha1:$sha1 magnet:".$self->{super}->Tools->encode_b32(pack("H*",$sha1)) )]);
				push(@MSG, [0, sprintf("Filecount              : %d", $so->GetFileCount)]);
				push(@MSG, [0, sprintf("Total size             : %.2f MB / %d piece(s)",       $stats->{total_bytes}/1024/1024,$stats->{total_chunks})]);
				push(@MSG, [0, sprintf("Completed              : %.2f MB / %d piece(s)",       $stats->{done_bytes}/1024/1024, $stats->{done_chunks})]);
				push(@MSG, [0, sprintf("Uploaded               : %.2f MB",                     $stats->{uploaded_bytes}/1024/1024)]);
				push(@MSG, [0, sprintf("Peers                  : Connected: %d / Active: %d",  $stats->{clients}, $stats->{active_clients})]);
				push(@MSG, [0, sprintf("Downloading since      : %s", ($so->GetSetting('createdat') ? "".localtime($so->GetSetting('createdat')) : 'Unknown'))]);
				push(@MSG, [0, sprintf("Last piece received at : %s", ($so->GetSetting('_last_recv') ? "".localtime($so->GetSetting('_last_recv')) : '-'))]);
				push(@MSG, [0, sprintf("Fully downloaded       : %s", ($stats->{done_chunks} == $stats->{total_chunks} ? "Yes" : "No"))]);
				push(@MSG, [0, sprintf("Download committed     : %s", ($so->CommitFullyDone ? 'Yes' : 'No'))]);
				push(@MSG, [0, sprintf("Uses sparsefile        : %s", ($so->UsesSparsefile ? 'Yes' : 'No'))]);
			}
			else {
				push(@SCRAP, $sha1);
			}
		}
	}
	else {
		$NOEXEC .= "Usage: details queue_id [queue_id2 ...]";
	}
	return({MSG=>\@MSG, SCRAP=>\@SCRAP, NOEXEC=>$NOEXEC });
}

##########################################################################
# Display current downloads
sub _Command_ViewDownloads {
	my($self) = @_;
	
	my @a            = ([1, "Dummy"]);
	my $qlist        = $self->{super}->Queue->GetQueueList;
	my $active_peers = 0;
	my $total_peers  = 0;
	my @order        = qw(type name hash peers pieces bytes percent ratio up down eta note);
	my $header       = { type=>'[Type]', name=>'Name                     ',
	                     hash=>'/================ Hash ================\\', peers=>' Peers',
	                     pieces=>' Pieces', bytes=>' Done (MB)', percent=>' Done',
	                     ratio=>'Ratio', up=>' Up', down=>' Down','note'=>'', eta=>'ETA' };
	my @items        = ({vrow=>1, rsep=>' '}, map({$header->{$_}} @order));
	push(@a, [undef, \@items]);
	
	foreach my $dl_type (sort(keys(%$qlist))) {
		foreach my $key (sort(keys(%{$qlist->{$dl_type}}))) {
			my $this_stats = $self->{super}->Queue->GetStats($key)      or $self->panic("$key has no stats!");                 # stats for this download
			my $this_so    = $self->{super}->Storage->OpenStorage($key) or $self->panic("Unable to open storage of $key");     # storage-object for this download
			my $xcolor     = 2;                                                                                                # default is red
			my $ll         = {};                                                                                               # this line
			my @xmsg       = ();                                                                                               # note-message
			
			# Set color and note-message
			if(my $ci = $this_so->CommitIsRunning) { push(@xmsg, "Committing file $ci->{file}/$ci->{total_files}, ".int(($ci->{total_size}-$ci->{written})/1024/1024)." MB left"); }
			if($this_so->CommitFullyDone)                                    { $xcolor = 3 }
			elsif($this_stats->{done_chunks} == $this_stats->{total_chunks}) { $xcolor = 4 }
			elsif($this_stats->{active_clients} > 0 )                        { $xcolor = 1 }
			elsif($this_stats->{clients} > 0 )                               { $xcolor = 0 }
			$active_peers += $this_stats->{active_clients};
			$total_peers  += $this_stats->{clients};
			
			if($self->{super}->Queue->IsPaused($key)) {
				my $ptxt = ($self->{super}->Queue->IsAutoPaused($key) ? "AutoPaused" : "Paused");
				push(@xmsg,$ptxt);
			}
			
			$self->{super}->Tools->GetETA($key);
			
			$ll->{type}   = sprintf("[%4s]",$dl_type);
			$ll->{name}   = $this_so->GetSetting('name');
			$ll->{hash}   = $key;
			$ll->{peers}  = sprintf("%3d/%2d",$this_stats->{active_clients},$this_stats->{clients});
			$ll->{pieces} = sprintf("%5d/%5d",$this_stats->{done_chunks}, $this_stats->{total_chunks});
			$ll->{bytes}  = sprintf("%7.1f/%7.1f", ($this_stats->{done_bytes}/1024/1024), ($this_stats->{total_bytes}/1024/1024));
			$ll->{percent}= sprintf("%4d%%", (($this_stats->{done_chunks}/$this_stats->{total_chunks})*100));
			$ll->{ratio}  = sprintf("%.2f", ($this_stats->{uploaded_bytes}/(1+$this_stats->{done_bytes})));
			$ll->{up}     = sprintf("%4.1f", $this_stats->{speed_upload}/1024);
			$ll->{down}   = sprintf("%4.1f", $this_stats->{speed_download}/1024);
			$ll->{eta}    = $self->{super}->Tools->SecondsToHuman($self->{super}->Tools->GetETA($key));
			$ll->{note}   = join(' ',@xmsg);
			$ll->{_color} = $xcolor;
			
			my @this = (undef, map({$ll->{$_}} @order));
			push(@a, [$xcolor,\@this]);
		}
	}
	

	
	
	$a[0] = [1, sprintf(" *** Upload: %6.2f KiB/s | Download: %6.2f KiB/s | Peers: %3d/%3d",
	                     ($self->{super}->Network->GetStats->{'sent'}/1024),
	                     ($self->{super}->Network->GetStats->{'recv'}/1024),
	                      $active_peers, $total_peers) ];
	
	
	return {MSG=>\@a, SCRAP=>[] };
}

##########################################################################
# Send out a notification
sub _Command_Notify {
	my($self, @args) = @_;
	my $string = join(' ', @args);
	$self->{super}->Admin->SendNotify($string);
	return {MSG=>[ [ 1, 'notification sent'] ], SCRAP=>[] };
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
	my($self,$NOW) = @_;
	
	foreach my $csock (keys(%{$self->{sockbuffs}})) {
		my $tsb = $self->{sockbuffs}->{$csock};
		
		if(defined($tsb->{repeat}) && ! $self->{super}->Network->GetQueueLen($tsb->{socket})) {
			$self->_Network_Data($tsb->{socket}, \$tsb->{repeat});
		}
		
		next if $tsb->{lastnotify} == $self->{notifyi}; # Does not need notification
		foreach my $notify (@{$self->{notifyq}}) {
			next if $notify->{id} <= $tsb->{lastnotify};
			$tsb->{lastnotify} = $notify->{id};
			if($tsb->{auth}) {
				my $cbuff = $tsb->{p}.$tsb->{cbuff};
				
				$self->{super}->Network->WriteDataNow($tsb->{socket}, ANSI_ESC."2K".ANSI_ESC."E");
				$self->{super}->Network->WriteDataNow($tsb->{socket}, Alert(">".localtime()." [Notification]: $notify->{msg}")."\r\n$cbuff");
			}
		}
	}
	return 2;
}


##########################################################################
# Accept new incoming connection
sub _Network_Accept {
	my($self,$sock) = @_;
	$self->info("New incoming connection from ".$sock->peerhost);
	$self->panic("Duplicate sockid?!") if defined($self->{sockbuffs}->{$sock});
	
	# DO = 253 ; DO_NOT = 254 ; WILL = 251 ; WILL NOT 252
	my $initcode =  chr(0xff).chr(251).chr(1).chr(0xff).chr(251).chr(3); # WILL echo + sup-go-ahead
	   $initcode .= chr(0xff).chr(253).chr(31);                          # DO report window size
	
	$self->{sockbuffs}->{$sock} = { cbuff => '', curpos=>0, history => [], h => 0, lastnotify => $self->{notifyi}, p => PROMPT, echo => 1,
	                                socket => $sock, repeat => undef, iac=>0, iac_args=>'', multicmd=>0, terminal=>{w=>80,h=>25},
	                                auth => $self->{super}->Admin->AuthenticateUser(User=>'', Pass=>''), auth_user=>undef };
	
	$self->{sockbuffs}->{$sock}->{p} = 'Login: ' unless $self->{sockbuffs}->{$sock}->{auth};
	
	my $motd     = "# Welcome to ".Green('Bitflu')."\r\n".$self->{sockbuffs}->{$sock}->{p};
	$self->{super}->Network->WriteDataNow($sock, $initcode.$motd);
}

##########################################################################
# Read data from network
sub _Network_Data {
	my($self,$sock,$buffref) = @_;
	
	my $new_data = ${$buffref};
	my $sb       = $self->{sockbuffs}->{$sock};
	my @exe      = ();
	
	foreach my $c (split(//,$new_data)) {
		my $nc = ord($c);
		
		if($nc == 0xFF) {
			$sb->{iac} = 0xFF; # InterpretAsCommand
		}
		elsif($sb->{iac}) {
			if($sb->{iac} == 0xff) {
				# first char after IAC marker -> command opcode
				if($nc == 240) {
					$sb->{iac} = 1; # end of subneg
					$self->UpdateTerminalSize($sb);
				}
				elsif($nc != 250) {
					$sb->{iac} = 2; # normal command (no subneg)
				}
				
				$sb->{iac_args} = $c;
			}
			else {
				$sb->{iac_args} .= $c;
			}
			$sb->{iac}--;
		}
		elsif($sb->{multicmd}) {
			$sb->{multicmd}--;
			next if $sb->{multicmd}; # walk all commands
			
			if ($nc == KEY_LEFT) {
				push(@exe, ['<',""]);
			}
			elsif($nc == KEY_RIGHT) {
				push(@exe, ['>',""]);
			}
			elsif($nc == KEY_C_LEFT) { # CTRL+<
				
				my $new_pos = 0; # start at beginning if everything else fails
				my $saw_nws = 0; # state of loop (sawNonWhiteSpace)
				
				for(my $i=($sb->{curpos}-1);$i>=0;$i--) { # start one behind current curpos (-1 if at beginning -> loop will do nothing)
					my $this_char = substr($sb->{cbuff},$i,1);
					$saw_nws = 1 if $this_char ne ' ';
					if($this_char eq ' ' && $saw_nws) {
						$new_pos = $i+1; # move one char ahead (*  [F]OO*)
						last;
					}
				}
				
				my $diff = ($sb->{curpos}-$new_pos);
				$self->panic if $diff < 0;
				map( push(@exe, ['<','']), (1..($sb->{curpos}-$new_pos)) );
			}
			elsif($nc == KEY_C_RIGHT) { # CTRL+>
				my $cb_len  = length($sb->{cbuff});
				my $new_pos = $cb_len;
				my $saw_nws = 0;
				
				for(my $i=$sb->{curpos};$i<$cb_len;$i++) {
					my $this_char = substr($sb->{cbuff},$i,1);
					$saw_nws = 1 if $this_char ne ' ';
					if($this_char eq ' ' && $saw_nws) {
						$new_pos=$i;
						last;
					}
				}
				
				my $diff = ($new_pos-$sb->{curpos});
				$self->panic if $diff < 0;
				map( push(@exe, ['>','']), (1..($diff)) );
			}
			elsif($nc == KEY_UP) {
				push(@exe, ['h', +1]);
			}
			elsif($nc == KEY_DOWN) {
				push(@exe, ['h', -1]);
			}
			
		}
		elsif($nc == 0x1b) { $sb->{multicmd} = 2;   }
		elsif($nc == 0x00) { }
		elsif($c eq "\n")  { }
		elsif($nc == 127 or $nc == 126 or $nc == 8) {
			# -> 'd'elete char (backspace)
			push(@exe, ['d', 1]);
		}
		elsif($nc == KEY_CTRLA) {
			map(push(@exe, ['<','']), (1..$sb->{curpos}));
		}
		elsif($nc == KEY_CTRLE) {
			map(push(@exe, ['>','']), ( $sb->{curpos}..(length($sb->{cbuff})-1) ));
		}
		elsif($c eq "\r") {
			# -> E'X'ecute
			if($sb->{auth}) {
				push(@exe, ['X',undef]);  # execute
			}
			else {
				push(@exe, ['!', undef]); # pass to authentication
			}
		}
		elsif($nc == KEY_TAB && $sb->{auth}) {
			push(@exe, ['T','']);
		}
		elsif($nc == KEY_CTRLD) {
			push(@exe, ['X','quit']);
		}
		elsif($nc == KEY_CTRLL) {
			push(@exe, ['X', 'clear']);
			push(@exe, ['a', $sb->{cbuff}]);
		}
		elsif($nc == KEY_CTRLC) {
			push(@exe, ['C', '']);
			push(@exe, ['R', undef]);
		}
		else {
			# 'a'ppend normal char
			push(@exe, ['a', $c]);
		}
	}
	
	while(defined(my $ocode = shift(@exe))) {
		my $tx = undef;
		my $oc = $ocode->[0];
		
		my $twidth        = $sb->{terminal}->{w};
		my $visible_chars = ( length($sb->{p}) + length($sb->{cbuff}) );
		my $visible_curpos= (length($sb->{p})+$sb->{curpos});
		my $line_position = ( $visible_curpos % $twidth );
		my $chars_left    = $twidth-$line_position;
		
		if($oc eq '<' or $oc eq '>') {
			
			if($oc eq '<' && $sb->{curpos} > 0) {
				$sb->{curpos}--;
				if($line_position == 0) { $tx = ANSI_ESC."1A".ANSI_ESC."${twidth}C"; }
				else                    { $tx = ANSI_ESC."1D";                       }
			}
			
			if($oc eq '>' && length($sb->{cbuff}) > $sb->{curpos}) {
				$sb->{curpos}++;
				if($chars_left == 1) { $tx = "\r\n";        }
				else                 { $tx = ANSI_ESC."1C"; }
			}
		}
		elsif($oc eq 'a') { # append a character
			# insert chars
			my $apn_length = length($ocode->[1]);
			substr($sb->{cbuff},$sb->{curpos},0,$ocode->[1]);
			$sb->{curpos}  += $apn_length;
			
			if($sb->{echo}) {
				if($chars_left > 1 && $sb->{curpos} == length($sb->{cbuff})) {
					# Avoid flickering and fix/workaround for an obscure aterm(?) bug
					$tx .= $ocode->[1];
				}
				else {
					# cursor not at end -> do it the hard way
					my $xc_after_line = int( ($visible_chars+$apn_length-1) / $twidth );
					my $xc_want_line  = int( ($visible_curpos+$apn_length) / $twidth  );
					my $xc_line_pos   = ( ($visible_curpos+$apn_length) % $twidth    );
					my $xc_line_diff  = $xc_after_line-$xc_want_line;
					
					$tx .= ANSI_ESC."0J".substr($sb->{cbuff},$sb->{curpos}-$apn_length); # delete from cursor and append data
					$tx .= " \r".ANSI_ESC."K"            if $chars_left == 1;            # warp curor
					$tx .= ANSI_ESC."${xc_line_diff}A"   if $xc_line_diff > 0;           # move to correct line (>0 : diff will be -1 on pseudo-warp)
					$tx .= "\r";
					$tx .= ANSI_ESC."${xc_line_pos}C"    if $xc_line_pos;                # move to corret linepos
				}
				
			}
		}
		elsif($oc eq 'd') { # Delete a character
			my $can_remove = ( $ocode->[1] > $sb->{curpos} ? $sb->{curpos} : $ocode->[1] );
			
			if($can_remove && $sb->{echo}) {
				$sb->{curpos} -= $can_remove;
				substr($sb->{cbuff},$sb->{curpos},$can_remove,"");                   # Remove chars from buffer
				
				my $xc_current_line = int( $visible_curpos / $twidth );              # Line of cursor
				my $xc_new_line     = int( ($visible_curpos-$can_remove)/$twidth );  # new line of cursor (after removing X chars)
				my $xc_new_lpos     = int( ($visible_curpos-$can_remove)%$twidth );  # new position on lline
				my $xc_append       = substr($sb->{cbuff},$sb->{curpos});            # remaining data
				my $xc_total_line   = int( ($visible_chars-$can_remove-1)/$twidth ); # total number of lines - cursor (1)
				my $xc_total_diff   = $xc_total_line - $xc_new_line;                 # remaining lines (after cursor)
				
				$tx  = "\r";                                                         # move to start of line
				$tx .= ANSI_ESC."${xc_current_line}A"   if $xc_current_line;         # move to line with prompt
				$tx .= ANSI_ESC."${xc_new_line}B"       if $xc_new_line;             # move cursor to new line
				$tx .= ANSI_ESC."${xc_new_lpos}C"       if $xc_new_lpos;             # move cursor to new position
				$tx .= ANSI_ESC."0J".$xc_append."\r";                                # remove from cursor + append remaining data
				$tx .= ANSI_ESC."${xc_new_lpos}C"       if $xc_new_lpos;             # fix line position (unchanged)
				$tx .= ANSI_ESC."${xc_total_diff}A"     if $xc_total_diff;           # go up X lines
			}
		}
		elsif($oc eq 'r') {
			unshift(@exe, ['a', $ocode->[1]]);
			unshift(@exe, ['d', length($sb->{cbuff})]);
			$sb->{curpos} = length($sb->{cbuff});
		}
		elsif($oc eq 'h') {
			my $hindx = $sb->{h} + $ocode->[1];
			if($hindx >= 0 && $hindx < int(@{$sb->{history}})) {
				$sb->{history}->[$sb->{h}] = $sb->{cbuff};
				$sb->{h} = $hindx;
				unshift(@exe, ['r', $sb->{history}->[$sb->{h}]]);
			}
		}
		elsif($oc eq 'X') {
			my $cmdout = $self->Xexecute($sock, (defined($ocode->[1]) ? $ocode->[1] : $sb->{cbuff}));
			if(!defined($cmdout)) {
				return undef; # quit;
			}
			# Make it an option "ignoredups"?
			my $prev = $sb->{history}->[1];
			if(defined $prev && $sb->{cbuff} eq $prev) { }
			elsif(length($cmdout) && length($sb->{cbuff})) {
				splice(@{$sb->{history}}, 0, 1, '', $sb->{cbuff});
				pop(@{$sb->{history}}) if int(@{$sb->{history}}) > $self->{super}->Configuration->GetValue('telnet_maxhist');
			}
			unshift(@exe, ['C',$cmdout]);
		}
		elsif($oc eq '!') { # -> login user
			if(length($sb->{auth_user}) == 0) {
				$sb->{auth_user} = ($sb->{cbuff} || "NULL");
				$sb->{echo}      = 0;
				$sb->{p} = "Password: ";
			}
			else {
				if( $self->{super}->Admin->AuthenticateUser(User=>$sb->{auth_user}, Pass=>$sb->{cbuff}) ) {
					$sb->{echo} = $sb->{auth} = 1;
					$sb->{p}    = $sb->{auth_user}.'@'.PROMPT;
					$self->info("Telnet login from user $sb->{auth_user} completed");
				}
				else {
					$self->info("Telnet login from user $sb->{auth_user} failed!");
					unshift(@exe, ['X', 'quit']);
					$sb->{p} = "Authentication failed, goodbye!\r\n\r\n"; # printed by the 'C' command below
				}
			}
			unshift(@exe, ['C', '']); # get a new line with a fresh prompt
		}
		elsif($oc eq 'C') {
			$sb->{h} = 0;
			$sb->{cbuff}  = '';
			$sb->{curpos} = 0;
			$tx = "\r\n".$ocode->[1].$sb->{p};
		}
		elsif($oc eq 'R') { # Re-Set Repeat code
			$sb->{repeat} = $ocode->[1];
		}
		elsif($oc eq 'T') {
			my $tabref = $self->TabCompleter($sb->{cbuff});
			if(defined($tabref->{append})) {
				unshift(@exe, ['a', $tabref->{append}]); # append suggested data
			}
			elsif(int(@{$tabref->{matchlist}}) > 1) { # multiple suggestions -> print them, hit CTRL+c and restore buffer
				unshift(@exe, ['r', "# ".join(" ",@{$tabref->{matchlist}})], ['C',''], ['a', $sb->{cbuff}]);
			}
		}
		else {
			$self->panic("Unknown telnet opcode '$oc'");
		}
		
		$self->{super}->Network->WriteDataNow($sock, $tx) if defined($tx);
	}
	
}

sub UpdateTerminalSize {
	my($self,$sb) = @_;
	
	my $arg = $sb->{iac_args};
	if( length($arg) == 6 && $arg =~ /^\xfa\x1f(..)(..)/ ) {
		$sb->{terminal}->{w} = (unpack("n",$1) || 1);
		$sb->{terminal}->{h} = (unpack("n",$2) || 1);
	}
}

##########################################################################
# Simple tab completition
sub TabCompleter {
	my($self,$inbuff) = @_;
	
	my $result     = { append => undef, matchlist => [] };
	my($cmd_part)  = $inbuff =~ /^(\S+)$/;
	my($sha_part)  = $inbuff =~ / ([_0-9A-Za-z-]*)$/;
	
	my @searchlist = ();
	my @hitlist    = ();
	my $searchstng = undef;
	
	if(defined($cmd_part)) {
		@searchlist = $self->{super}->Admin->GetCompletion('cmd');
		$searchstng = $cmd_part;
	}
	elsif(defined($sha_part)) {
		@searchlist = $self->{super}->Admin->GetCompletion('arg1');
		$searchstng = $sha_part;
	}
	
	if(int(@searchlist)) {
		foreach my $t (@searchlist) {
			if($t =~ /^$searchstng(.*)$/) {
				push(@hitlist, $t);
			}
		}
		
		if(int(@hitlist) == 1) { # just a single hit
			$result->{append} = substr($hitlist[0],length($searchstng))." ";
		}
		elsif(int(@hitlist)) {
			my $bestmatch = $self->FindBestMatch(@hitlist);
			
			if($bestmatch eq $searchstng) { # no 'better' matches -> set matchlist
				$result->{matchlist} = \@hitlist;
			}
			else { # possible part-list
				$result->{append} = substr($bestmatch,length($searchstng));
			}
		}
	}
	return $result;
}


##########################################################################
# Returns a common wordprefix..
sub FindBestMatch {
	my($self, @hitlist) =@_;
	
	my $match = shift(@hitlist);
	
	foreach my $word (@hitlist) {
		my $i = 0;
		while(++$i && $i<=length($word) && $i<=length($match)) {
			if(substr($word,0,$i) ne substr($match,0,$i)) {
				last;
			}
		}
		$match = substr($word,0,$i-1); # $i is always > 0
	}
	return $match;
}


sub Xexecute {
	my($self, $sock, $cmdstring) = @_;
	
	my @xout = ();
	my $sb   = $self->{sockbuffs}->{$sock};
	
	foreach my $cmdlet (_deToken($cmdstring)) {
		my $type            = $cmdlet->{type};
		my ($command,@args) = @{$cmdlet->{array}};
		
		
		if($command eq 'repeat' && int(@args)) {
			my (undef,$rcmd) = $cmdstring =~ /(^|;)\s*repeat (.+)/;
			if(length($rcmd)) {
				$sb->{repeat} = $rcmd." # HIT CTRL+C TO STOP\r\n";
				push(@xout, Green("Executing '$rcmd' each second, hit CTRL+C to stop\r\n"));
				last;
			}
		}
		
		if($type eq "pipe") {
			if($command eq "grep") {
				my $workat = (pop(@xout) || '');
				my $filter = ($args[0]   || '');
				my $result = '';
				my $match  = 1;
				
				if($filter eq '-v') {
					$match  = 0;
					$filter = ($args[1] || '');
				}
				
				foreach my $line (split(/\n/,$workat)) {
					if( ($line =~ /$filter/gi) == $match) {
						$result .= $line."\n";
					}
				}
				if(length($result) > 0) {
					push(@xout,$result);
				}
				else {
					push(@xout, "\00");
				}
			}
			elsif($command eq "sort") {
				my $workat = (pop(@xout) || '');
				my $mode   = ($args[0]   || '');
				
				if($mode eq '-r') { $mode = sub { $a cmp $b } }
				else              { $mode = sub { $b cmp $a } }
				push(@xout, join("\n", sort( {&$mode} split(/\n/,$workat)), "\00"));
			}
			elsif($command eq "head" or $command eq "tail") {
				my $workat   = (pop(@xout) || '');
				my @cmd_buff = (split(/\n/,$workat));
				my ($limit)  = ($args[0]||'') =~ /^-(\d+)/;
				$limit     ||= 10; # 10 is the default
				$limit       = int(@cmd_buff) if int(@cmd_buff) < $limit;
				
				if($command eq "head") {
					@cmd_buff = splice(@cmd_buff,0,$limit);
				}
				else {
					@cmd_buff = splice(@cmd_buff,-1*$limit);
				}
				
				push(@xout, join("\n", (@cmd_buff,"\00")));
			}
			else {
				push(@xout, Red("Unknown pipe command '$command'\r\n"));
			}
		}
		elsif($command =~ /^(q|quit|exit|logout)$/) {
			$self->_Network_Close($sock);
			$self->{super}->Network->RemoveSocket($self,$sock);
			return undef;
		}
		else {
			my $exe  = $self->{super}->Admin->ExecuteCommand($command,@args);
			my $buff = '';
			my @msg  = @{$exe->{MSG}};
			my $spstr= $self->_GetSprintfLayout(\@msg,$sb->{terminal}->{w}); # returns '%s' in the worst case
			
			
			foreach my $alin (@msg) {
				my $cc = ($alin->[0] or 0);
				my $cv = $alin->[1];
				
				# array mode: first item specifies the variable-width row
				# the rest is just arguments
				$cv = sprintf($spstr, splice(@$cv,1)) if ref($cv) eq 'ARRAY';
				
				   if($cc == 1)         { $buff .= Green($cv)  }
				elsif($cc == 2)         { $buff .= Red($cv)    }
				elsif($cc == 3)         { $buff .= Yellow($cv) }
				elsif($cc == 4)         { $buff .= Cyan($cv)   }
				elsif($cc == 5)         { $buff .= Blue($cv)   }
				else                    { $buff .= $cv;        }
				$buff .= "\r\n";
				
				if($cc == 0xff)         { $buff = Clear($cv)   } # Special opcode: Clear the screen
			}
			push(@xout, $buff);
		}
	}
	
	return join("",@xout);
}

##########################################################################
# Parses $msg and creates a sprintf() string
sub _GetSprintfLayout {
	my($self,$msg, $twidth) = @_;
	
	my @rows = ();    # row-width
	my $xstr = '%s';  # our sprintf string - this is a fallback
	my $vrow = undef; # variable-sized row
	my $rsep = '?';
	
	foreach my $alin (@$msg) {
		next if ref($alin->[1]) ne 'ARRAY'; # plain string -> no row layout
		
		# element 0 should be undef or a hashref with {vrow=>VARIABLE_ROW, rsep=>CHAR}
		my $vdef = $alin->[1]->[0];
		if(defined($vdef) && !defined($vrow)) {
			($vrow,$rsep) = ($vdef->{vrow}, $vdef->{rsep});
		}
		
		for(my $i=1; $i<int(@{$alin->[1]});$i++) {
			my $l = length($alin->[1]->[$i]);
			$rows[$i-1] = $l if ($rows[$i-1] || 0) <= $l;
		}
	}
	
	if(int(@rows)) { # -> we got a row layout - prepare special sprintf() string
		for(0..1) {
			$xstr = join($rsep, map({"%-${_}.${_}s"} @rows));
			my $spare = $twidth - length(sprintf($xstr,@rows));
			last if $spare >= 0; # was already ok or fixup was good
			$rows[$vrow] += $spare;
			$rows[$vrow] = 1 if $rows[$vrow] < 1;
		}
	}
	
	#$self->info("str=$xstr, vrow=$vrow, twidth=$twidth");
	return $xstr;
}

##########################################################################
# Close down TCP connection
sub _Network_Close {
	my($self,$sock) =  @_;
	$self->info("Closing connection with ".$sock->peerhost);
	delete($self->{sockbuffs}->{$sock});
}

##########################################################################
# Parse tokens

sub _deToken {
	my($line) = @_;
	my @parts    = ();
	my @commands = ();
	my $type     = 'cmd';
	
	my $in_apostrophe = 0;
	my $in_escape     = 0;
	my $buffer        = undef;
	$line            .= ";"; # Trigger a flush
	
	for(my $i=0; $i<length($line); $i++) {
		my $char = substr($line,$i,1);
		if($in_escape) {
			$buffer .= $char;
			$in_escape = 0;
		}
		elsif($char eq "\\") {
			$in_escape = 1;
		}
		elsif($in_apostrophe) {
			if($char eq '"') { $in_apostrophe = 0; }
			else             { $buffer .= $char;   }
		}
		else {
			if($char eq '"') {
				$in_apostrophe = 1;
				$buffer .= '';
			}
			elsif($char =~ /\s|;|\|/) {
				push(@parts,$buffer) if defined($buffer);
				$buffer = undef;
			}
			else {
				$buffer .= $char;
			}
			#####
			if($char =~ /;|\|/) {
				my @xcopy = @parts;
				push(@commands,{type=>$type, array=>\@xcopy}) if int(@parts);
				@parts = ();
				$type  = ($char eq ';' ? 'cmd' : 'pipe');
			}
		}
	}
	
	return @commands;
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

sub Clear {
	my($s) = @_;
	return ANSI_ESC.'H'.ANSI_ESC.'2J'.$s;
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

sub debug { my($self, $msg) = @_; $self->{super}->debug("Telnet  : ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info("Telnet  : ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic("Telnet  : ".$msg); }
sub stop { my($self, $msg) = @_; $self->{super}->stop("Telnet  : ".$msg); }



1;
