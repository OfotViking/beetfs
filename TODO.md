# TODO

## Pick up library changes without remounting

At the moment, the only way to see music imported or changed in beets is to unmount and mount again (`systemctl restart beetfs`). That causes three problems:

- **Stale handles:** anything holding the mount open keeps pointing at the old mount, which is dead once its process is killed. For example, smbd keeps the share root and every file a client has touched open for the whole session. Samba clients then get errors until they reconnect. The README works around this with `smbcontrol smbd close-share`, but music servers and shells `cd`'d into the mount are affected the same way.
- **Empty mountpoint:** until the new tree is built, the mountpoint is empty. A service that scans it during that time can drop the music from its own library.
- **Downtime:** the whole tree is rebuilt while nothing is mounted, so a large library is unavailable for that long.

The fix is to rebuild the inode table **inside the running process** and swap it in. The mount never goes away, so open handles stay valid. Since 0.12, inode numbers are derived from the entry instead of the build order (item id for files, hash of parent inode and name for directories), so an entry that hasn't changed keeps its inode across a rebuild.

### Triggering a rebuild

- **SIGHUP** to the mount process. Run a trio task next to `pyfuse3.main` that waits on `trio.open_signal_receiver(signal.SIGHUP)`. `beet mount` forks one child per mountpoint and the parent exits, so systemd's `$MAINPID` is only a guess. Either write a pid file per mountpoint, or use `ExecReload=/usr/bin/pkill -HUP -f 'beet mount'`.
- **Automatic** (optional, could be added later): poll the mtime of `library.db` (and `library.db-wal`, if present) every 30–60 s. Rebuild only after it has stopped changing for a while, so a long `beet import` doesn't trigger a rebuild per album. This would make `systemctl restart beetfs` unnecessary for new music.
- Add `ExecReload=` to the README's systemd unit, and change the README's advice from `restart` to `reload`.

### Rebuilding

- Build the new table in a worker thread (`trio.to_thread.run_sync`), so requests keep being served from the old table meanwhile. `build_inode_table` would have to return a new connection instead of writing to `self.inode_db`. The connection must use `check_same_thread=False`, because it is built in one thread and queried from the event loop thread.
- Swap `self.inode_db` on the event loop thread in a single assignment. Every handler reads the table through `inode_query`, so after the swap they all see the new table.
- Path formats and queries come from the config loaded at `beet mount` time. A reload does not re-read the beets config, so changes to path formats still need a restart. Re-reading `config` in place is possible, but plugins such as `inline` register their fields when they load.
- Log how many entries were added, removed and changed, and keep the collision warnings.

### Telling the kernel

The kernel caches lookups (`entry_timeout`) and attributes (`attr_timeout`), and Samba and other clients cache on top of that. After the swap, compare the old and new tables:

- **An entry `(parent, name)` that is gone, or now has a different inode:** call `pyfuse3.invalidate_entry_async(parent, name)`. Don't use the synchronous `invalidate_entry` from inside the event loop, because it can deadlock against a request in progress.
- **A directory whose children changed:** call `pyfuse3.invalidate_inode(dir)`. This drops its cached listing.
- **A file whose item changed:** call `pyfuse3.invalidate_inode(inode)`. This drops cached attributes and pages. Tag edits change the header size, and so the file size.
- Everything else keeps its inode and needs nothing.

### Things to watch out for

- **Item ids can be reused.** beets' `items.id` is `INTEGER PRIMARY KEY` without `AUTOINCREMENT`, so SQLite can hand out the id of the most recently deleted item again. The same inode would then be a different file. The invalidation above covers the kernel. For NFS exports, also bump `EntryAttributes.generation` for an inode whose item changed, so that old NFS file handles are rejected instead of reaching the new file.
- **Hash collisions:** a directory that lost or won a hash collision can change inode across a rebuild. This is rare, and the invalidation covers it too.
- **readdir across a swap:** `readdir` reads the rows again on every call and uses the row index as the offset. A listing that spans a swap could skip or repeat entries (see the existing TODO in `readdir`). Snapshot the rows in `opendir`, keyed by a new fh, use them in `readdir`, and drop them in `releasedir`.
- **Caches:**
  - `album_art_cache` and `no_album_art` are keyed by directory inode, so they stay correct. Clear `no_album_art` on reload anyway, so new art shows up straight away.
  - `header_info` and `header_data` are keyed by item id, and their key includes the source mtime and a digest of the tags, so they need nothing. Entries for removed items could be pruned.
- **Load:** a rebuild evaluates every path template again and holds the GIL while doing so, so requests slow down during it. Audio reads already run in worker threads, but keep an eye on playback while a large library rebuilds. If it stutters, consider rebuilding in a subprocess and passing the result back as an SQLite file.

### Testing

The podman setup used to find the Samba problem (Debian container with beets, pyfuse3 and Samba, shared to Windows with `net use \\127.0.0.1\Music /TCPPORT:4450`) is a good harness:
- Hold a folder open with a `FileSystemWatcher`.
- `beet import` an album and `beet modify` a title, then send SIGHUP.
- Check that the new album appears, the renamed file has its new name, and the old handles still read.
- Check that nothing fails 25 s later, after the Windows client caches have expired, without running `close-share`.

## Change a file's mtime when its tags change

A served file's `st_mtime` is the source file's mtime (`getattr`). When beets doesn't write tags to the files (`write: no`, read-only or seeded music), `beet modify` only changes `library.db`. The served file then gets the new tags and possibly a new size, but keeps its old mtime. Media players and indexers that rescan files with a newer mtime (most of them, e.g. Lyrion, Jellyfin, Plex) never pick up the edit, so a full rescan is the only way. The same goes for backup and sync tools that compare mtimes.

### What the mtime should be

Serve `max(source mtime, time the item's served tags last changed)`. Neither beets nor beetfs records the second value:

- Item's `mtime` field: no. beets only updates it when it reads or writes the file, so it stays the same on a database-only `modify`.
- Flexible attribute set from a beets event: no. The beetfs plugin would set it from `database_change` in whichever `beet` command makes the change. But storing it fires the event again, and the event doesn't say which fields changed, so non-tag changes (e.g. `artpath`) would bump it too. Any change made while the plugin isn't loaded would also be missed.
- **beetfs's own record:** yes. `source_key` already computes `tags_digest` of exactly the tags that are served. Keep `item_id -> (digest, changed_at)`, and when the digest differs from the stored one, set `changed_at = now`. This catches every change, however it was made.

### Persisting it

That record has to survive remounts, or every file would look changed after each restart. That would mean a full rescan by every player, which is worse than the current behaviour. Keep it in a small SQLite file of beetfs's own, e.g. `beetfs.db` in beets' config directory (`config.config_dir()`), not in `library.db` (see the in-memory inode table for why). With several mounts, each process writes it, so use WAL mode and short transactions, or one file per mountpoint.

- **First run:** there's no record yet, so store the current digests with `changed_at = 0`. Nothing gets newer than its source file until it is actually edited.
- **When to check:** checking only on `getattr`/`open` means an edit is only noticed when something stats the file. A player scanning by mtime does stat it, so the next scan sees the new mtime. That is enough, and it avoids walking the whole library. A bulk check at mount time, or on reload (see above), could catch edits made while nothing was mounted, but costs a pass over all items.
- **Serving the new values:** use `changed_at` for `st_mtime`, and for `st_ctime` as well, since metadata changed. Don't set it on the album-art and directory entries, but consider bumping a directory's mtime when one of its files changes. Some scanners only descend into directories whose mtime changed.
- **Kernel cache:** call `pyfuse3.invalidate_inode` when `changed_at` moves, so the kernel's cached attributes and pages are dropped (the headers changed too).
- **Pruning:** remove records for items that are gone from the library.

### Testing the mtime

Use the podman harness:
1. Note a file's mtime over SMB and from `stat` in the container.
2. Run `beet modify -W title=...` (database only).
3. Check that the mtime moves forward and the new title is served.
4. Remount and check that the mtime stays the same as after step 3, and doesn't jump again.

## Don't report the mount as full

`statfs` reports a made-up 1,000,000 blocks with none free, and 1,000,000 inodes with none free (`f_bfree = f_bavail = f_ffree = 0`). `df` therefore shows the mount at 100% use, and so does `df -i`. Monitoring that discovers filesystems by itself (Zabbix's "Linux filesystems" discovery, also Prometheus' node_exporter, Nagios `check_disk`) raises "disk full" and "no free inodes" alerts for it.

### Fix

Pass the statistics of the filesystem holding the music through (`os.statvfs` of beets' `directory`, run in a worker thread since the music may be on NFS), and set only `f_namemax` ourselves. The numbers then mean something: how full the disk under the music is. The mount only alerts when that disk really is full, and that disk alerts too. mergerfs and other FUSE filesystems that sit on top of real storage do the same.

- **Fallback:** if the stat fails (the share is gone), return all zeros, as a FUSE filesystem without `statfs` does. `df` hides filesystems with no blocks unless run with `-a`, and percentage checks have nothing to compute.
- **Don't** report `f_bfree = f_blocks` (0% used) to quiet the alerts. That claims the space is free, and writes fail anyway.
- **Mount type:** also add the mount option `subtype=beetfs`, so the type shows as `fuse.beetfs` instead of plain `fuse` in `/proc/mounts`, `df -T` and `findmnt`. Monitoring can then exclude it by type without catching every FUSE mount, e.g. Zabbix's `{$VFS.FS.FSTYPE.NOT_MATCHES}` macro on the host. Check that `findmnt -S beetfs` in the README still works: it matches the source (`fsname`), which stays the same.

### Until then

Exclude the mountpoints in Zabbix: add them to `{$VFS.FS.FSNAME.NOT_MATCHES}` on the host, e.g. `^(/dev|/sys|/run|/proc|.+/shm$|/home/foo/beets/.*)`. Keep the template's default patterns in the regex, because a host macro replaces the template's value instead of adding to it.

### Testing statfs

- `df -h` and `df -i` on the mountpoint show the same sizes and use as the music directory's filesystem.
- `df -T` shows `fuse.beetfs`.
- `findmnt -S beetfs` still lists the mounts.
- With the music directory unmounted under a running beetfs, `df` (without `-a`) hides the mount instead of failing.
