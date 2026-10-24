# TODO

## Reloading: what's left

Since 0.13, SIGHUP rebuilds the tree in place (`Operations.reload`), and the README's systemd unit has `ExecReload=`. Still open:

- **Automatic reload:** poll the mtime of `library.db` (and `library.db-wal`, if present) every 30–60 s. Rebuild only after it has stopped changing for a while, so a long `beet import` doesn't trigger a rebuild per album. This would make `systemctl reload beetfs` unnecessary after an import.
- **NFS generation numbers:** beets' `items.id` is `INTEGER PRIMARY KEY` without `AUTOINCREMENT`, so SQLite can hand out the id of the most recently deleted item again, and the same inode is then a different file. The reload invalidates it in the kernel, but for NFS exports, also bump `EntryAttributes.generation` for an inode whose item changed, so that old NFS file handles are rejected instead of reaching the new file.
- **Load:** a rebuild evaluates every path template again and holds the GIL while doing so, so requests slow down during it. Keep an eye on playback while a large library rebuilds. If it stutters, consider rebuilding in a subprocess and passing the result back as an SQLite file.
- **Re-reading the config:** a reload keeps the path formats and query loaded at `beet mount` time. Re-reading `config` in place is possible, but plugins such as `inline` register their fields when they load.
- **Samba directory leases:** Samba 4.22+ lets clients cache listings until it breaks their directory lease, and it doesn't notice a reload. The README turns them off (`smb3 directory leases = no`). Telling Samba instead would need inotify events from the FUSE mount: according to libfuse's docs, `invalidate_entry(parent, name, deleted=inode)` (`notify_delete`) raises `IN_DELETE` for removed and renamed entries (untested), but nothing raises `IN_CREATE` for new ones.

Testing used the podman harness (Debian container with beets, pyfuse3 and Samba, shared to Windows with `net use \127.0.0.1\Music /TCPPORT:4450`). Note that `pkill -HUP -f 'beet mount'` inside `sh -c '...'` also matches, and kills, that shell; use `'[b]eet mount'` there.

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
- **Kernel cache:** call `invalidate_inodes` (from the reload) when `changed_at` moves, so the kernel's cached attributes and pages are dropped (the headers changed too).
- **Pruning:** remove records for items that are gone from the library.

### Testing the mtime

Use the podman harness:
1. Note a file's mtime over SMB and from `stat` in the container.
2. Run `beet modify -W title=...` (database only).
3. Check that the mtime moves forward and the new title is served.
4. Remount and check that the mtime stays the same as after step 3, and doesn't jump again.
