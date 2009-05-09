package Bitflu::Rss;
#
# This file is part of 'Bitflu' - (C) 2009 Adrian Ulrich
#
# Released under the terms of The "Artistic License 2.0".
# http://www.perlfoundation.org/legal/licenses/artistic-2_0.txt
#

#
# Fixme: PerRSS Delay
#        Seen{} Leaks memory (no cleanup)
#

use strict;
use XML::Simple;
use Storable;

use constant _BITFLU_APIVERSION => 20090501;
use constant MAX_RSS_SIZE       => 1024*256;
use constant CLIPBOARD_PFX      => 'rss_';

##########################################################################
# Registers the HTTP Plugin
sub register {
	my($class, $mainclass) = @_;
	my $self = { super => $mainclass, next_dload=>0 };
	bless($self,$class);
	
	$mainclass->AddRunner($self);
	return $self;
}

##########################################################################
# Regsiter admin commands
sub init {
	my($self) = @_;
	$self->{super}->Admin->RegisterCommand('rss', $self, '_Command_RSS', "Change/View RSS settings. See 'help rss' for details",
	  [ [undef, "Bitflu can fetch RSS feeds."] ] );
	return 1;
}


##########################################################################
# Called from ->Super
sub run {
	my($self,$NOW) = @_;
	$self->warn("RSS plugin is running!");
	
	my $ql       = $self->{super}->Queue->GetQueueList;  # Get a full queue list
	my @http     = keys(%{$ql->{http}});                 # Array with HTTP-Only keys
	my @nlinks   = ();
	foreach my $this_sha (@http) {
		if(my $so = $self->Super->Storage->OpenStorage($this_sha)) {
			my $this_name = $so->GetSetting('name');
			my $this_size = $so->GetSetting('size');
			
			if($this_size < MAX_RSS_SIZE && $this_name =~ /^internal\@(.+)/) {
				my $rss_key    = $1;
				my $rss_buff   = $self->_ReadFile($so);
				$self->Super->Admin->ExecuteCommand('cancel' , $this_sha);
				$self->Super->Admin->ExecuteCommand('history', $this_sha, 'forget');
				
				my $filtered_links = $self->_FilterFeed(RssKey=>$rss_key, Buckets=>$self->_XMLParse($rss_buff));
				push(@nlinks, @$filtered_links);
			}
			
		}
	}
	
	foreach my $rsslink (@nlinks) {
		$self->Super->Admin->ExecuteCommand('load', $rsslink);
	}
	
	if($self->{next_dload} <= $NOW) {
		$self->{next_dload} = $NOW + 60*30;
		foreach my $rsskey ($self->_GetRssKeys) {
			my $ref = $self->_GetRssFromKey($rsskey);
			my $xurl = $ref->{Name};
			$xurl =~ s/^http:\/\//internal\@$rsskey:\/\//i;
			
			$self->warn("Fetching RSS-Feed: $xurl ($rsskey)");
			$self->Super->Admin->ExecuteCommand('load', $xurl);
		}
	}
	
	return 15;
}


sub _Command_RSS {
	my($self, @args) = @_;
	my @MSG = ();
	
	my $a0 = (shift(@args) or '');
	my $a1 = (shift(@args) or '');
	
	if($a0 eq 'add' && $a1) {
		if($self->_GetRssFromName($a1)) {
			push(@MSG,[2, "Job $a1 exists (see 'rss list')"]);
		}
		elsif($a1 =~ /^http:\/\//i) {
			$self->_SetRss(Name=>$a1);
			push(@MSG, [1, "rss-job $a1 has been added"]);
		}
		else {
			push(@MSG, [2, "Invalid URL (must start with http://)"]);
		}
	}
	elsif($a0 eq 'update') {
		$self->{next_dload} = 0;
		push(@MSG, [1, "rss-download triggered"]);
	}
	elsif($a0 eq 'list' or $a0 eq '') {
		push(@MSG, [3, "Registered RSS feeds:"]);
		foreach my $rsskey ($self->_GetRssKeys) {
			my $ref = $self->_GetRssFromKey($rsskey);
			push(@MSG,[4, " $rsskey : $ref->{Name}"]);
		}
	}
	elsif(my $rf = $self->_GetRssFromKey($a0)) {
		if($a1 eq 'flush') {
			$rf->{Seen} = {};
			$self->_SetRss(%$rf);
			push(@MSG,[1, "$a0: history has been flushed"]);
		}
		elsif($a1 eq 'show') {
			push(@MSG,[0, "Name/Url  : $rf->{Name}"]);
			push(@MSG,[0, "Whitelist : $rf->{Whitelist}"]);
			push(@MSG,[0, "Seen items:"]);
			foreach my $v (sort values(%{$rf->{Seen}})) {
				push(@MSG, [0, " $v"]);
			}
		}
		elsif($a1 eq 'delete') {
			$self->_DeleteRssKey($a0);
			push(@MSG,[1, "$a0: deleted rss feed"]);
		}
		elsif($a1 eq 'whitelist' && defined($args[0])) {
			$rf->{Whitelist} = $args[0];
			$self->_SetRss(%$rf);
			push(@MSG,[1, "$a0: whitelist set to '$rf->{Whitelist}'"]);
		}
		else {
			push(@MSG, [2, "Unknown subcommand, see 'rss help' for details"]);
		}
	}
	else {
		push(@MSG, [2, "Unknown command, see 'rss help' for details"]);
	}
	
	return({MSG=>\@MSG, SCRAP=>[]});
}

##########################################################################
# Returns a list with all RSS feeds
sub _GetRssKeys {
	my($self) = @_;
	my @cblist = $self->Super->Storage->ClipboardList;
	my $match  = CLIPBOARD_PFX;
	my @keys   = ();
	foreach my $rsskey (@cblist) {
		next unless $rsskey =~ /^$match/;
		push(@keys,$rsskey);
	}
	return @keys;
}

##########################################################################
# Return RSS reference for given key
sub _GetRssFromName {
	my($self,$name) = @_;
	$self->_GetRssFromKey($self->_GetRssKeyFromName($name));
}

##########################################################################
# Returns an RSS feed from it's own key
sub _GetRssFromKey {
	my($self,$key) = @_;
	my $ref = $self->Super->Storage->ClipboardGet($key);
	if($ref) {
		my $feed = {};
		eval { $feed = Storable::thaw($ref) }; # Try to unfreeze
		return $feed;
	}
	else {
		return undef;
	}
}

##########################################################################
# Removes a key from clipboard
sub _DeleteRssKey {
	my($self,$key) = @_;
	my $exists = $self->_GetRssFromKey($key) or return undef;
	$self->Super->Storage->ClipboardRemove($key);
}

##########################################################################
# Store/Update/Set an RSS entry in clipboard
sub _SetRss {
	my($self,%args) = @_;
	my $name  = delete($args{Name})      or $self->panic("No name?!");
	my $seen  = delete($args{Seen})      or {};
	my $wlist = delete($args{Whitelist}) or '';
	my $xref  = { Name=>$name, Whitelist=>$wlist, Seen=>$seen };
	$self->Super->Storage->ClipboardSet($self->_GetRssKeyFromName($name), Storable::nfreeze($xref));
}

##########################################################################
# Return clipboard key from name
sub _GetRssKeyFromName {
	my($self,$name) = @_;
	return CLIPBOARD_PFX.$self->Super->Tools->sha1_hex($name);
}

##########################################################################
# Parses an RSS file and returns rss-buckets
sub _XMLParse {
	my($self,$buffer) = @_;
	
	my $xp      = XML::Simple->new;
	my $xmlref  = $xp->XMLin($buffer);
	my @buckets = ();
	
	if(ref($xmlref->{channel}) eq 'HASH' && ref($xmlref->{channel}->{item}) eq 'ARRAY') {
		foreach my $ref (@{$xmlref->{channel}->{item}}) {
			next unless ref($ref) eq 'HASH';
			my $this_link  = $ref->{link} or next;
			my $this_guid  = $self->Super->Tools->sha1_hex($ref->{guid} or $this_link);
			my $this_title = ($ref->{title} or $this_link);
			push(@buckets, { link=>$this_link, guid=>$this_guid, title=>$this_title });
		}
	}
	return \@buckets;
}

sub _FilterFeed {
	my($self,%args) = @_;
	my $rss_key  = delete($args{RssKey})  or $self->panic("No RSS Key?!");
	my $rss_buck = delete($args{Buckets}) or $self->panic("No RSS Buckets!");
	my $rss_feed = $self->_GetRssFromKey($rss_key);
	my @to_fetch = ();
	if($rss_feed && exists($rss_feed->{Name})) {
		foreach my $buck (@$rss_buck) {
			if(exists($rss_feed->{Seen}->{$buck->{guid}}) or (length($rss_feed->{Whitelist}) &&  $buck->{title} !~ /$rss_feed->{Whitelist}/i ) ) {
				# void
			}
			else {
				warn "+ $buck->{link}\n";
				push(@to_fetch, $buck->{link});
			}
			$rss_feed->{Seen}->{$buck->{guid}} = $buck->{title};
		}
		$self->_SetRss(%$rss_feed);
	}
	return \@to_fetch;
}



sub _ReadFile {
	my($self,$so) = @_;
	my $buff = '';
	my $size = $so->GetSetting('size');
	for(my $i=0;;$i++) {
		my($this_chunk) = $so->GetFileChunk(0,$i);
		last if !defined($this_chunk); # Error?!
		$buff .= $this_chunk;
		last if length($buff) == $size; # file is complete
		}
	return $buff;
}

sub Super { my($self) = @_; return $self->{super}; }
sub debug { my($self, $msg) = @_; $self->{super}->debug(ref($self).": ".$msg); }
sub info  { my($self, $msg) = @_; $self->{super}->info(ref($self).": ".$msg);  }
sub warn  { my($self, $msg) = @_; $self->{super}->warn(ref($self).": ".$msg);  }
sub panic { my($self, $msg) = @_; $self->{super}->panic(ref($self).": ".$msg); }


1;
