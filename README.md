# beetfs

beetfs mounts a FUSE that provides all your music with the desired tags and metadata applied to the files without touching the actual files underneath.

## Directory structure

The directory tree of the mount is built from path formats laid out exactly like beets' own [`paths`](https://beets.readthedocs.io/en/stable/reference/config.html#path-format-configuration):
```
beetfs:
    paths:
        default: $albumartist/$album%aunique{} ($year)/%if{$multidisc,$disc}/$track $title
        singleton: Singletons/$artist - $title
        comp: Compilations/$album%aunique{}/$track $title
        albumtype:soundtrack: Soundtracks/$album/$track $title
```
Each track goes where the first format whose query it matches puts it: `singleton` matches tracks not in an album, `comp` compilations, any other key is a beets query (here `albumtype:soundtrack`), and `default` takes the rest. Tracks can sit at different depths, and as in beets, an empty directory level is left out. `$multidisc` is not a beets field, so the format above needs it defined with the `inline` plugin; it then only adds a disc directory for albums with more than one disc:
```
plugins: beetfs inline
item_fields:
    multidisc: 1 if disctotal > 1 else 0
```

Without `beetfs.paths`, the mount uses beets' own `paths`. `path_format: <format>` is a shorthand for `paths` with only a `default`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
```

Every track needs its own path. If two tracks produce the same path (two editions of an album, or two discs that both start with "01 Intro"), the later one is served with its beets id appended, e.g. `01 Intro [1207].flac`, and a warning is logged at mount time. To get clean names instead, make the format unique the way beets' own default paths do:
```
beetfs:
    path_format: '%first{$albumartist}/$album%aunique{} ($year)/%if{$multidisc,$disc-}$track $title'
```

## Mounting

To mount the filesystem, simply give the mountpoint as an argument to `beet mount`, e.g. `beet mount ~/Music/beetfs`. A beets query can optionally be passed with `-q` to filter the database, e.g. `beet mount -q 'soundtrack:1' ~/Music/Soundtracks`

One or more mountpoints can be specified in the config file with beets queries and will be mounted with just `beet mount`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
    mounts:
        /home/foo/beets/bmnt:
        /home/foo/beets/soundtracks: soundtrack:1
        /home/foo/beets/music: soundtrack:0
```

## Unmounting

`beet mount` starts one background process per mountpoint and returns straight away, so there is nothing to Ctrl+C. To stop a mount, unmount it:
```
fusermount3 -u ~/Music/beetfs
```
(or `umount ~/Music/beetfs` as root). The process serving that mount exits on its own once it is unmounted. With several mountpoints, unmount each one; `findmnt -S beetfs` lists the ones currently mounted.

If it fails with `Device or resource busy`, something still has a file or directory open in the mount (a music player, a shell `cd`'d into it); `fuser -vm ~/Music/beetfs` shows what. Close it and try again, or use `fusermount3 -uz` to detach the mount now and finish unmounting once it is no longer in use.

Don't stop it by killing the process: that leaves the mountpoint behind, failing with `Transport endpoint is not connected`, until it is unmounted with `fusermount3 -u` as above.

## Picking up library changes

The directory tree is built when the filesystem is mounted, and rebuilt by itself when the beets library changes: once `library.db` has been left alone for a minute, so a long `beet import` leads to one rebuild at the end rather than one per album. Music imported, changed or removed in beets therefore shows up in the mount a minute or so later. To wait a different number of seconds, or to turn this off:
```
beetfs:
    auto_reload: 60     # or no
```
To rebuild straight away, e.g. without automatic reloading, send the mount processes `SIGHUP`:
```
pkill -HUP -f 'beet mount'
```
Each mount rebuilds its tree in the background, keeps serving the old one meanwhile, and swaps the new one in. The mount stays in place, so music players, Samba and shells `cd`'d into it carry on without noticing, apart from seeing the changes. The log shows how many files were added, removed, moved and retagged.

A reload keeps the beets config that was loaded at mount time, so after changing path formats or other settings, unmount and run `beet mount` again. Inode numbers come from the beets item ids and the directory paths, so they stay the same across reloads and remounts: tools that track files by inode (backup tools, NFS exports) don't see the whole library as changed.

### File times after tag edits

A file's modification time is its source file's, or the time its tags last changed in beets if that is later. So when beets only changes `library.db` (`write: no`, or music that must stay untouched), music servers and other tools that rescan files by modification time still pick up the edit, without a full rescan. The folders above the file get the newer time too.

A tag change is noticed when the tree is rebuilt, so about a minute after `beet modify` (see above). beetfs remembers each track's tags and when they last changed in `beetfs.db` next to the beets config (`~/.config/beets/beetfs.db`), so the times stay the same across remounts, and mounts share it. The first time, every file keeps its source time. If the file can't be used, beetfs logs a warning and serves the source times.

## Disk usage and monitoring

`df` on a mount shows the size and usage of the filesystem holding the music (beets' `directory`), so the mount only looks full when that disk really is. If the music directory can't be reached, the mount reports no blocks at all and plain `df` leaves it out.

The mounts have the type `fuse.beetfs` (in `df -T`, `findmnt` and `/proc/mounts`), so monitoring that finds filesystems by itself can skip them by type. In Zabbix, add `fuse.beetfs` to the host's `{$VFS.FS.FSTYPE.NOT_MATCHES}` macro, keeping the template's default patterns in the regex, since a host macro replaces the template's value rather than adding to it.

## Starting at boot

A systemd service can mount everything in `beetfs.mounts` at boot. This one waits until the music directory beets points at (here `/mnt/music`) is mounted and readable, which covers a local disk, NFS or sshfs share that comes up late or not at all: until then, the checks fail and systemd tries again every 30 seconds. Save it as `/etc/systemd/system/beetfs.service`, replacing the user `foo`, the paths and the mountpoints with yours:
```ini
[Unit]
Description=beetfs
Wants=network-online.target
After=network-online.target remote-fs.target
# keep retrying for as long as the music isn't there
StartLimitIntervalSec=0

[Service]
Type=forking
User=foo
# the music directory must be mounted (drop this line if it is a plain
# directory on the root filesystem) and must not be empty
ExecStartPre=/usr/bin/mountpoint -q /mnt/music
ExecStartPre=/bin/sh -c 'ls -A /mnt/music | grep -q .'
# clear a mount left behind by a crash; one line per mountpoint
ExecStartPre=-/usr/bin/fusermount3 -uz /home/foo/beets/music
ExecStart=/home/foo/.local/bin/beet mount
# beet mount returns before the mounts are up; only count as started once they
# are, so services ordered after this one see the music. One line per mountpoint
ExecStartPost=/bin/sh -c 'until mountpoint -q /home/foo/beets/music; do sleep 1; done'
# rebuild the trees in place to pick up library changes (see Picking up library changes)
ExecReload=/usr/bin/pkill -HUP -f 'beet mount'
# one line per mountpoint
ExecStop=-/usr/bin/fusermount3 -uz /home/foo/beets/music
Restart=on-failure
RestartSec=30
# building the directory tree of a large library can take a while
TimeoutStartSec=15min

[Install]
WantedBy=multi-user.target
```
Then enable and start it:
```
sudo systemctl daemon-reload
sudo systemctl enable --now beetfs
```
- It is a system service running as your user rather than a `systemctl --user` one, because user services can't wait for system mounts such as NFS.
- beetfs mounts with `allow_other`, which a non-root user may only do when `/etc/fuse.conf` contains the line `user_allow_other`.
- `ExecStart` must use the `beet` beets is installed as (`which beet`; with pipx it is `~/.local/bin/beet`). If `library.db` or your beets config also live on the share, check those paths in `ExecStartPre` too.
- If the share is in `/etc/fstab` (NFS, or sshfs as `fuse.sshfs` with `_netdev`), `remote-fs.target` also makes sure beetfs is unmounted before the share at shutdown. An sshfs share mounted by hand is only covered by the checks, and must be mounted with `allow_other` if another user mounted it.
- `sudo systemctl reload beetfs` picks up library changes straight away (they are otherwise picked up after a minute), `sudo systemctl restart beetfs` remounts (needed after changing the beets config), `sudo systemctl stop beetfs` unmounts everything, and `journalctl -u beetfs` shows the log. The stop uses `fusermount3 -uz`, so it doesn't fail on a busy mount (see [Unmounting](#unmounting)).

### Services that use the mount

A service that reads the music from beetfs, such as a music server, should only start once beetfs is mounted. Otherwise it can find an empty directory and drop the music from its own library. Here is how to do that for [Lyrion Music Server](https://github.com/LMS-Community/slimserver-platforms/blob/public/9.2/debian/lyrionmusicserver.service). Other services work the same way.

1. Add a drop-in to the service with `sudo systemctl edit lyrionmusicserver`, rather than editing its unit file, which package upgrades replace:
   ```ini
   [Unit]
   Requires=beetfs.service
   After=beetfs.service
   ```
   With `After=`, Lyrion waits for beetfs to finish starting, and the `ExecStartPost` line above makes that mean "mounted". With `Requires=`, Lyrion doesn't start at all without beetfs, and it is stopped or restarted along with it. Lyrion is also stopped before beetfs, so it has closed its files by the time beetfs unmounts, at shutdown or on `systemctl restart beetfs`.
2. If beetfs fails to start (say the NFS share isn't up yet), Lyrion's start fails too. beetfs keeps retrying, but Lyrion has `Restart=no` and would stay down. To start Lyrion again whenever beetfs comes up, add a drop-in to beetfs with `sudo systemctl edit beetfs` (needs systemd 249 or newer):
   ```ini
   [Unit]
   Upholds=lyrionmusicserver.service
   ```
   This also restarts Lyrion if you stop it while beetfs is running, so to keep it stopped, stop beetfs: `sudo systemctl stop beetfs` stops both.
3. Lyrion runs as its own user, `squeezeboxserver`. It can read the mount because beetfs mounts with `allow_other`, which needs `user_allow_other` in `/etc/fuse.conf` (see above).
4. Point Lyrion's music folder at the beetfs mountpoint, e.g. `/home/foo/beets/music`, not at the real music directory.

Run `sudo systemctl daemon-reload` after editing, if `systemctl edit` didn't already do it. `systemctl list-dependencies lyrionmusicserver` should now list `beetfs.service`.

### Sharing the mount with Samba

Samba 4.22 and newer let clients cache folder listings until Samba tells them something changed (SMB3 directory leases). Samba doesn't notice when beetfs reloads, so Windows clients keep showing the old listing of every folder they have open until they disconnect. Turn directory leases off in the `[global]` section of `smb.conf`, so clients list a folder again after at most 10 seconds:
```ini
# beetfs changes the listings without Samba noticing
smb3 directory leases = no
```

`systemctl reload beetfs` needs nothing else. A restart does: Samba keeps the share's directory, and whatever a client has opened in it, open for as long as the client stays connected. After `systemctl restart beetfs` those still point at the old mount, which is gone: browsing keeps working for a few seconds from caches, then Windows shows errors such as "The parameter is incorrect" or "The directory name is invalid" until it disconnects. To have Samba drop its connections to the share once the new mount is up, add this to the `[Service]` section of `beetfs.service`, below the `ExecStartPost` line that waits for the mount, with `Music` replaced by the share's name:
```ini
# make Samba clients reconnect to the new mount; + runs it as root
ExecStartPost=-+/usr/bin/smbcontrol smbd close-share Music
```
Clients reconnect by themselves, and Samba doesn't need restarting. Samba doesn't need to start after beetfs either, as it only looks at the mountpoint when a client connects.

## Album art

Each directory holding an album's tracks (or a single singleton) gets a `cover.jpg` (or `.png`), taken from the first of:
1. the art beets has for the album (`artpath`, e.g. from the `fetchart` plugin)
2. a cover file next to the music (`cover`, `folder`, `front` or `album` `.jpg`/`.jpeg`/`.png`)
3. art embedded in one of the album's files

Changes show up within five minutes, without remounting.

beets normally saves fetched art next to the music, which fails if the music is read-only and modifies it if it isn't (e.g. when seeding the files). Set `art_dir` to keep beets' album art in a separate directory instead:
```
beetfs:
    art_dir: ~/.local/share/beets/art
plugins: beetfs fetchart
fetchart:
    auto: yes
```
Art is stored there as `<album id>.jpg`. This applies to every beets command while the beetfs plugin is enabled. An image that already sits next to the music (found by fetchart's `filesystem` source) is copied, never moved, and beets won't delete art files in the music directories when replacing art. Don't use `embedart`: it writes the art into the music files.

## Install

Install this fork into the same Python environment as beets. With beets installed through pipx:
```
pipx inject --force beets git+https://github.com/OfotViking/beetfs
```
or with pip: `pip install git+https://github.com/OfotViking/beetfs`. (The `beets-beetfs` package on PyPI is the upstream version, without this fork's changes.) pyfuse3 needs the libfuse3 headers to build (`libfuse3-dev`/`fuse3-devel`) and the fuse3 runtime to mount.

Then add `beetfs` to `plugins` in your `config.yaml` along with the options above. To check which copy beets loads: `beet config -p` shows your config, and `python -c "import beetsplug.beetfs as b; print(b.__file__)"` (run with beets' Python, e.g. `~/.local/pipx/venvs/beets/bin/python`) shows the plugin file.
