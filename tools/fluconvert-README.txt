The default storage plugin of bitflu 0.4x is not compatible with the old 0.3x format, so you'll need
to use fluconvert.pl to migrate your data.


Migration Example:

  /mnt/bitflu-0.3/           # Bitflu 0.3x rootdir
  /mnt/bitflu-0.3/workdir    # Bitflu 0.3x workdir
  /mnt/bitflu-0.4/           # Bitflu 0.4x rootdir

$ telnet 0 4001 # send 'die' to shutdown the running bitflu instance
$ cd /mnt/bitflu-0.4/
$ ./tools/fluconvert.pl ../bitflu-0.3/workdir ./workdir
$ ./bitflu.pl &  # Start bitlfu 0.4x
$ telnet 0 4001 # send 'die' to shutdown bitflu 0.4x (it just created it's own storage dir)
$ ./tools/fluconvert.pl --movedata ../bitflu-0.3/workdir ./workdir

The migration is now complete. You can start bitflu 0.4x again.
