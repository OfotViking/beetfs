# beetfs

beetfs mounts a FUSE that provides all your music with the desired tags and metadata applied to the files without touching the actual files underneath.

beetfs takes a config option for building the directory structure of your beets filesystem. The format is the same that is used in `paths`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
```

Every track needs its own path. If two tracks produce the same path (two editions of an album, or two discs that both start with "01 Intro"), the later one is served with its beets id appended, e.g. `01 Intro [1207].flac`, and a warning is logged at mount time. To get clean names instead, make the format unique the way beets' own default paths do:
```
beetfs:
    path_format: '%first{$albumartist}/$album%aunique{} ($year)/%if{$multidisc,$disc-}$track $title'
```

To mount the filsystem, simply give the mountpoint as an argument to `beet mount`, e.g. `beet mount ~/Music/beetfs`. A beets query can optionally be passed with `-q` to filter the database, e.g. `beet mount -q 'soundtrack:1 ~/Music/Soundtracks`

One or more mountpoints can be specified in the config file with beets queries and will be mounted with just `beet mount`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
    mounts:
        /home/foo/beets/bmnt:
        /home/foo/beets/soundtracks: soundtrack:1
        /home/foo/beets/music: soundtrack:0
```

## Album art

Each album directory in the mount gets a `cover.jpg` (or `.png`), taken from the first of:
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
