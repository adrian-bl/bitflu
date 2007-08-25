package Bitflu::Cron;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2006-2007 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

use strict;
use constant QUEUE_SCAN => 10; # Sorry.. do not mess with this using the Configuration plugin.

##########################################################################
# Register this plugin
sub register {
	my($class,$mainclass) = @_;
	my $self = { super   => $mainclass , autoload_dir => $mainclass->Configuration->GetValue('workdir').'/autoload',
	             autoload_scan => 300, lastrun => 0, next_autoload_scan => 0, next_queue_scan => 0 };
	bless($self,$class);
	
	foreach my $funk qw(autoload_dir autoload_scan) {
		my $this_value = $mainclass->Configuration->GetValue($funk);
		if(defined($this_value)) {
			$self->{$funk} = $this_value;
		}
		else {
			$mainclass->Configuration->SetValue($funk,$self->{$funk});
		}
	}
	
	$mainclass->AddRunner($self);
	return $self;
}

##########################################################################
# Register  private commands
sub init {
	my($self) = @_;
	
	unless(-d $self->{autoload_dir}) {
		$self->info("Creating autoload_dir '$self->{autoload_dir}'");
		mkdir($self->{autoload_dir}) or $self->panic("Unable to create autoload_dir '$self->{autoload_dir}' : $!");
	}
	$self->{super}->Admin->RegisterCommand('autoload',   $self, '_Command_Autoload', "Scan $self->{autoload_dir} for new files");
	return 1;
}


##########################################################################
# Kick autoloader
sub _Command_Autoload {
	my($self) = @_;
	$self->{next_autoload_scan} = 0;
	return({CHAINSTOP=>1, MSG=>[[1, "Autoload triggered"]]});
}

sub run {
	my($self) = @_;
	my $NOW = $self->{super}->Network->GetTime;
	return undef if $NOW == $self->{lastrun};
	
	if($self->{next_autoload_scan} <= $NOW) {
		$self->{next_autoload_scan} = $NOW + $self->{super}->Configuration->GetValue('autoload_scan');
		$self->info("Scanning autoload directory '$self->{autoload_dir}' for new downloads");
		if( opendir(ALH, $self->{autoload_dir}) ) {
			while(defined(my $dirent = readdir(ALH))) {
				$dirent = $self->{autoload_dir}."/$dirent";
				next unless -f $dirent;
				my $exe = $self->{super}->Admin->ExecuteCommand('load',$dirent);
				if($exe->{CHAINSTOP} != 0) {
					$self->info("Autoloaded '$dirent'");
					unlink($dirent) or $self->warn("Unable to remove $dirent : $!");
				}
				else {
					$self->warn("Unable to autoload file '$dirent'. Please remove this file");
				}
			}
			closedir(ALH) or $self->panic("Unable to close dir we just opened: $!");
		}
		else {
			$self->warn("Unable to open autoload_dir '$self->{autoload_dir}' : $!");
		}
	}
	if($self->{next_queue_scan} <= $NOW) {
		$self->{next_queue_scan} = $NOW + QUEUE_SCAN;
	#	$self->info("Should scan the queue and notify about completed downloads, cancel well seeded ones and commit some");
	}
	
	
}



sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self)."[Cron]: ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self)." [Cron]: ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self)." [Cron]: ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self)."[Cron]: ".$msg); }



1;
