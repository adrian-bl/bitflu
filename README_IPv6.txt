~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 

1.  Introduction
1.1 Requirements
1.2 'Support Matrix'
1.3 Pitfalls

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


1.2 'Support Matrix'


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
      | Torrent: DHT/Kademlia     |   0% | Not implemented / No protocol extension defined (yet)             |
      +------------------------------------------------------------------------------------------------------+


1.3 Pitfalls

 * Uhm.. i don't think there are any :-)
