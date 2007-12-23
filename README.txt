Welcome to bitflu!

Bitflu is a 'download-daemon' for *NIX-like operating systems, written
in Perl5. Currently BitTorrent and HTTP are supported.

Report bugs to <adrian@blinkenlights.ch>

~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 
FAQ Index:

1. Getting started
1.1	What systems does bitflu run on?
1.2	What do i need to run bitflu?
1.3	How do i install or upgrade bitflu?
1.4	How do i start bitflu?

2. Configuration
2.1	Introduction
2.2	Ports you must open
2.3	Chrooting bitflu

3. Using Bitflu
3.1	Connecting to the telnet gui
3.2	Starting a new download
3.3	Viewing existing downloads
3.4	Working with multifile downloads
3.5	Removing a download
3.6	How to seed a torrent




~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

1. Getting started

1.1	What systems does bitflu run on?

Bitflu should work fine on all *NIX-like operating systems with a
working Perl5 installation. Development and regular testing is done
on Linux 2.6 (x86 and ppc), but people reported success running
it on FreeBSD and OpenBSD.

Note: If you are running bitflu on Solaris with a 32bit Perl5
version, bitflu will be limited to ~255 file descriptors.
(This is a perl limitation. Using a 64bit version 'fixes'
 this problem)


1.2	What do i need to run bitflu?

Not much:

	* Perl 5 (5.8 >= recommended)
	* The module 'Digest::SHA' or 'Digest::SHA1'
	  (Note: Perl 5.10 includes Digest::SHA, so bitflu should
	         work 'Out of the box')
	* A correctly configured 'firewall'
	  (You must accept/forward $torrent_port [udp AND tcp])
	* Some spare disk-space and bandwidth ;-)


1.3	How do i install or upgrade bitflu?

Installation:

	* mkdir /foo/bitflu
	* mv bitflu-*.tgz /foo/bitflu
	* cd /foo/bitflu
	* tar -xzf bitflu-*.tgz ; rm bitflu-*.tgz
	* mv bitflu-*/bitflu.pl bitflu-*/plugins .
	Note: You must install Digest::SHA1 for bitflu.
	Most distributions provide a package for this module, if
	your distribution doesnt, try:
	# perl -MCPAN -e 'install Digest::SHA1'

Upgrade:

	Just replace 'bitflu.pl' and the 'plugins' folder
	with the new version:
	* mv bitflu-*.tgz /foo/bitflu
	* cd /foo/bitflu
	* rm -rf bitflu.pl plugins # removes the old version
	* tar -xzf bitflu-*.tgz ; rm bitflu-*.tgz
	* mv bitflu-*/bitflu.pl bitflu-*/plugins .

1.4	How do i start bitflu?

	* cd /foo/bitflu
	* ./bitflu.pl
	You can now connect to bitflu using
	'telnet 127.0.0.1 4001'

Note: Do not run bitflu via an absolute path (eg. /foo/bitflu/bitflu.pl)
      unless you changed the 'workdir' value into an absolute path.
      (The default is './workdir' (= the folder 'workdir' at the current
       directory))



2. Configuration

2.1	Introduction

At startup, bitflu will try to read the the file '.bitflu.config'
(This can be changed using the --config switch). Most values can
be changed while running using the 'config' command:
	* config show        : Displays current configuration
	* config set foo bar : Set 'foo' to 'bar'

Some values (such as torrent_port) cannot be configured 'online'.
To change this value, you must stop bitflu and edit the configuration 
file using a text editor (such as vi).


2.2	Ports you must open

The ports used by the BitTorrent plugin must be reachable from
the internet (otherwise DHT won't work and downloads will be slow).
Per default, bitflu uses port 6688 (torrent_port):
tcp:6688 is used for 'BitTorrent connections', udp:6688 is 
used for Kademlia (finding sources)


2.3	Chrooting bitflu

Chrooting bitflu is recommended for security reasons.
(Do you trust my code? I do not ;-) )

This is how you could create the chroot jail:
(Replace '12345' with an unprivileged UID/GID)

	# Create the 'base directory'
	* mkdir /foo/bitflu/chroot
	* chown root:root /foo/bitflu/chroot
	* mkdir /foo/bitflu/chroot/workdir
	* chown 12345:12345 /foo/bitflu/chroot/workdir

	# Create directory for some system libs
	* mkdir /foo/bitflu/chroot/etc
	* mkdir /foo/bitflu/chroot/lib
	* mkdir /foo/bitflu/chroot/dev

	# ..and populate them:
	* cp /etc/hosts /etc/nsswitch.conf /etc/protocols \
	     /etc/resolv.conf /foo/bitflu/chroot/etc
	* cp /lib/libnss_* /lib/libresolv.so* /foo/bitflu/chroot/lib
	* mknod /foo/bitflu/chroot/dev/urandom c 1 9

	# Edit/Create the '.bitflu.config' file and add:
	  chroot    = /foo/bitflu/chroot
	  runas_uid = 12345
	  runas_gid = 12345
	
Depending on your operating system you may need to add some additional
files (to get DNS working). Use 'strace' (or truss) to find them :-)


3. Using Bitflu

3.1	Connecting to the telnet gui

Just connect to '127.0.0.1' on port 4001 using a normal telnet client:
	telnet 127.0.0.1 4001

The telnet client uses colored output, supports shell-like history and has 
tab completition for some commands (and queue id's)


3.2	Starting a new download

Use the 'load' command to start downloading a localy saved torrent file:
	load /tmp/foo.torrent

You can also download files via HTTP:
	load http://bitflu.workaround.ch/ChangeLog.txt
	Note 1: Bitflu will fail to download HTTP files if the webserver
	        failed to specify the size.

	Note 2: Downloading a .torrent file via HTTP causes bitflu to
	        'auto-import' the file itself

Torrent files can also be placed into the 'autoload folder'. Bitflu will re-
scan this folder each 300 seconds. You can force a re-scan using the
'autoload' command


3.3	Viewing existing downloads

Just type 'vd' (or 'ls'):

 *** Upload:  37.18 KiB/s | Download:  34.99 KiB/s | Peers:  74/208
>[Type] Name                     /================= Hash =================\ Peers |   Pieces   |   Done (MB)    | Done | Ratio|  Up  | Down |
 [ bt ] Name of the download     |1234567890abcdef1234567890abcdef01234567|  8/16 |  951/ 2222 | 1902.0/ 4442.8 |  42% | 0.40 |  6.5 | 15.6 | 


	[ bt ]        : Type of the download
	Name of..     : Name of the download
	12345678..    : The Queue-ID (SHA1 of the Torrent)
	8/16          : We got 16 peers and can download from 8
	951/2222      : The download has 2222 pieces, we got 951
	1902.0/4442.8 : 1902.0MB of 4442.8MB are downloaded
	42%           : ..that's 42%
	0.40          : We got an Up/Down ratio of 0.4 (40%)
	Up            : We are uploading 6.5 KiB/s
	Down          : We are downloading 15.6 KiB/s


3.4	Working with multifile downloads

Torrents can include multiple files. Use 'files list queue_id' to display them:
	files list 1234567890abcdef1234567890abcdef01234567
	Note: The telnet-gui supports tab completition, so you can just
	      tybe: 'files list 123<TAB>'

You can also commit/assemble single files from a not-yet finished download:
	pcommit 1234567890abcdef1234567890abcdef01234567 1 3-5
	This would assemble file 1, 3, 4 and 5


3.5	Removing a download

Use the 'cancel' command to remove a download from queue (also stops seeding).
Bitflu will auto-cancel downloads itself after reaching a ratio >= 1.5
You can change this setting on a global basis using 'config set autocancel 3.5'
Setting a per-torrent value is also possible using the 'autocancel' command:
	autocancel 1234567890abcdef1234567890abcdef01234567 off
	(See 'help autocancel' for more information)


3.6	How to seed a torrent

Bitflu cannot yet create .torrent files itself, but using 
maketorrent-console (or Azureus) works just fine:

	* maketorrent-console http://example.com/foo toshare
	* Load the created torrent into bitflu
	* move (or copy) toshare/* into workdir/import/
	* run 'import torrent-hash-to-import'
	* Bitflu will now mark the download as 'completed'
	* Turn autocancel off (because you are seeding it!)
	  autocancel torrent-hash off
	* done!

