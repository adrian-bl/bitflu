~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 

1.  Introduction
1.1 Requirements
1.2 'Support Matrix'
1.3 Pitfalls
1.4 Running an IPv6-Only node (aka. disable IPv4)

~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 



1. Introduction

This document gives you an overview about bitflus IPv6 support
and how to use it.



1.1 Requirements

 * A public IPv6 IP.
    This might sound hard but it isn't: If your ISP supports native IPv6
    you are fine. If it doesn't: Complain and start using IPv6 via tunnels:
    
    You can get an IPv6 tunnel at http://go6.net/ or google for 6to4
    if you have a static IPv4 (You'll get a static IPv6 via 6to4)

 * Bitflu >= 0.80 is required

 * You must install 2 additional Perl-Modules:
     - IO::Socket::INET6
     - Socket6

 * 'ipv6' in your bitflu.config must be set to '1' (default)


1.2 'Support Matrix' as of Bitflu 1.30


      +------------------------------------------------------------------------------------------------------+
      | Component                 | Done | Comment                                                           |
      +------------------------------------------------------------------------------------------------------+
      | Network: Binding to IPv6  | 100% | Fully supported                                                   |
      +------------------------------------------------------------------------------------------------------+
      | Network: IPv6 Connections | 100% | Fully supported                                                   |
      +------------------------------------------------------------------------------------------------------+
      | Torrent: On-Wire-Protocol | 100% | Fully supported                                                   |
      +------------------------------------------------------------------------------------------------------+
      | Torrent: TCP-Tracker      | 100% | Fully supported                                                   |
      +------------------------------------------------------------------------------------------------------+
      | Torrent: UDP-Tracker      |  50% | Works somewhat but protocol specification isn't finished yet      |
      +------------------------------------------------------------------------------------------------------+
      | Torrent: Peer-Exchange    | 100% | Fully supported: Bitflu can send and receive IPv6 peers           |
      +------------------------------------------------------------------------------------------------------+
      | Torrent: DHT/Kademlia     | 100% | Bitflu >= 0.95 implements BEP-32                                  |
      +------------------------------------------------------------------------------------------------------+


1.3 Pitfalls

 * Uhm.. i don't think there are any :-)


1.4 Running an IPv6-Only node (aka. disable IPv4)

It is possible to disable all IPv4 support in bitflu, however: you shouldn't do this unless you are
running some experements.

But here it is:

 Step 1: Stop Bitflu
 
 Step 2: Edit bitflu.pl and search for 'use constant KILL_IPV4'
 
 Step 3: Change
   use constant KILL_IPV4    => 0;
    ..into..
   use constant KILL_IPV4    => 1;
  
 Step 4: Edit yor .bitflu.config
   * Verify that ipv6 support is enabled
   * Change telnet_bind to (eg.) ::1
  
 Step 5: Start bitflu again
  
Bitflu will now not establish any IPv4 connections and closes any incoming IPv4 connection.
Native IPv4-UDP traffic will get discard.




