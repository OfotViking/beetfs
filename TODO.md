# TODO

## Reloading: what's left

Since 0.13, SIGHUP rebuilds the tree in place (`Operations.reload`), and the README's systemd unit has `ExecReload=`. Since 0.15, a mount also reloads by itself once `library.db` has been left alone for `beetfs.auto_reload` seconds. Still open:

- **NFS generation numbers:** beets' `items.id` is `INTEGER PRIMARY KEY` without `AUTOINCREMENT`, so SQLite can hand out the id of the most recently deleted item again, and the same inode is then a different file. The reload invalidates it in the kernel, but for NFS exports, also bump `EntryAttributes.generation` for an inode whose item changed, so that old NFS file handles are rejected instead of reaching the new file.
- **Load:** a rebuild evaluates every path template again and holds the GIL while doing so, so requests slow down during it. Keep an eye on playback while a large library rebuilds. If it stutters, consider rebuilding in a subprocess and passing the result back as an SQLite file.
- **Re-reading the config:** a reload keeps the path formats and query loaded at `beet mount` time. Re-reading `config` in place is possible, but plugins such as `inline` register their fields when they load.
- **Samba directory leases:** Samba 4.22+ lets clients cache listings until it breaks their directory lease, and it doesn't notice a reload. The README turns them off (`smb3 directory leases = no`). Telling Samba instead would need inotify events from the FUSE mount: according to libfuse's docs, `invalidate_entry(parent, name, deleted=inode)` (`notify_delete`) raises `IN_DELETE` for removed and renamed entries (untested), but nothing raises `IN_CREATE` for new ones.

Testing used the podman harness (Debian container with beets, pyfuse3 and Samba, shared to Windows with `net use \\127.0.0.1\Music /TCPPORT:4450`). Note that `pkill -f 'beet mount'` also matches, and signals, a shell whose command line or script contains `beet mount`, e.g. `sh -c '...'`; use `'[b]eet mount'` in the pattern and run such scripts from a file.
