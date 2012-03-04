package Bitflu::Debug;
####################################################################################################
#
# This file is part of 'Bitflu' - (C) 2011 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.opensource.org/licenses/artistic-license-2.0.php
#

use strict;
use constant _BITFLU_APIVERSION  => 20110912;


##########################################################################
# Register this plugin
sub register {
	my($class,$mainclass) = @_;
	my $self = { super   => $mainclass };
	bless($self,$class);
	return $self;
}

##########################################################################
# Register  private commands
sub init {
	my($self) = @_;
	
	
	$self->{super}->Admin->RegisterCommand('profiler_start',   $self, '_Command_profiler_start',  "start NYTprof");
	$self->{super}->Admin->RegisterCommand('profiler_stop',    $self, '_Command_profiler_stop',  "stop NYTprof");
	
	return 1;
}




sub _Command_profiler_start {
	my($self) = @_;
	DB::enable_profile();
	return({MSG=>[[1, "profiler enabled"]], SCRAP=>[]});
}
sub _Command_profiler_stop {
	my($self) = @_;
	DB::finish_profile();
	return({MSG=>[[1, "profiler enabled"]], SCRAP=>[]});
}



1;
