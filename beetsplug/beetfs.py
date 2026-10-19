"""Module providing the beetfs plugin for beets"""

from collections import OrderedDict
from io import BytesIO
import errno
import functools
import hashlib
import itertools
import logging
import os
import sqlite3
import stat
import tempfile
import time
import trio

import pyfuse3
from mediafile import MediaFile

from beets import config
from beets.library import Album, Item, parse_query_string
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand, UserError
from beets.util import bytestring_path, normpath, sanitize_path

beetfs_logger = logging.getLogger('beets')

# Inode numbers are derived from what an entry is rather than handed out in build
# order, so they stay the same across remounts and tools that track files by inode
# (backup tools, NFS exports, media indexers) don't see the whole library change:
# a file is its beets item id with ITEM_INODE_FLAG set, a directory a hash of its
# parent's inode and its name (below ITEM_INODE_FLAG), and a directory's cover art
# the directory's inode with ALBUM_ART_INODE_FLAG set. All stay below 2**63, as
# sqlite integers are signed.
ROOT_INODE = 1
ITEM_INODE_FLAG = 1 << 61
ALBUM_ART_INODE_FLAG = 1 << 62

# keys of a paths section that stand for a query, as in beets' own paths
PATH_FORMAT_QUERIES = {'singleton': 'singleton:true', 'comp': 'comp:true'}

COVER_FILE_NAMES = ['cover.jpg', 'cover.jpeg', 'cover.png',
                    'folder.jpg', 'folder.jpeg', 'folder.png',
                    'front.jpg', 'front.jpeg', 'front.png',
                    'album.jpg', 'album.jpeg', 'album.png']

# how many rewritten headers to keep in memory; headers can carry embedded art,
# so only their lengths are kept for every item
HEADER_CACHE_SIZE = 32

# how many directories' cover art to keep in memory
ALBUM_ART_CACHE_SIZE = 64

# how long found (or missing) cover art is trusted before looking again, so art
# fetched while mounted shows up without a remount
ALBUM_ART_RECHECK_SECONDS = 300

def is_within(path, directory):
    """whether path is directory or inside it"""
    path, directory = os.path.abspath(path), os.path.abspath(directory)
    return os.path.commonpath([path, directory]) == directory

def redirect_album_art(art_dir):
    """makes beets keep album art in art_dir, named by album id, instead of in the
    album's own directory next to the music.

    beets has no setting for this: art_filename is always joined onto the album
    directory, and a leading '/' is sanitized away. That makes fetchart unusable
    when the music must stay untouched (read-only storage, seeded torrents).
    Album.art_destination and Album.set_art are wrapped instead; this affects
    every beets command run with the beetfs plugin and art_dir configured.
    """
    art_dir = normpath(art_dir)
    os.makedirs(art_dir, exist_ok=True)
    original_set_art = getattr(Album, 'beetfs_original_set_art', Album.set_art)
    # where beets puts downloads (beets.util.get_module_tempdir), e.g. fetchart's
    temp_dir = os.path.join(bytestring_path(tempfile.gettempdir()), b'beets')

    def art_destination(album, image, item_dir=None): # pylint: disable=unused-argument
        ext = os.path.splitext(bytestring_path(image))[1].lower()
        return os.path.join(art_dir, str(album.id).encode() + ext)

    def set_art(album, path, copy=True):
        path = bytestring_path(path)
        # fetchart moves the image into place. Only let it move its own downloads;
        # anything else (e.g. a cover.jpg found next to the music) is copied so the
        # music directories are never modified.
        if not copy and not is_within(path, temp_dir):
            copy = True
        # beets deletes the album's previous art file first; never let that be a
        # file in a music directory (art set before art_dir was configured)
        if album.artpath and not is_within(bytestring_path(album.artpath), art_dir):
            album.artpath = None
        return original_set_art(album, path, copy)

    Album.beetfs_original_set_art = original_set_art
    Album.art_destination = art_destination
    Album.set_art = set_art

# Disk and beets-database work runs in worker threads so one slow request (a header
# build, a disk spinning up) doesn't hold up every other request, e.g. a playing
# stream. Header/art building gets its own small pool so a library scan can't take
# all the threads that audio reads need.
METADATA_THREADS = 4

def read_source(path, offset, size):
    """reads size bytes at offset from a source file; blocking, runs in a worker thread"""
    with open(path, 'rb') as bfile:
        bfile.seek(offset)
        return bfile.read(size)

def directory_inode(parent, name, attempt=0):
    """the inode of directory name in parent; a later attempt gives the next candidate,
    for when that one is taken"""
    key = f'{parent}\0{name}\0{attempt}'.encode('utf-8', 'surrogateescape')
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return int.from_bytes(digest, 'big') & (ITEM_INODE_FLAG - 1)

def tags_digest(tags):
    """a compact fingerprint of an item's tags, used as part of the header cache key"""
    return hashlib.blake2b(repr(sorted(tags.items())).encode(), digest_size=16).digest()

def fuse_handler(func):
    """turns unexpected exceptions in a FUSE handler into an errno for that one request

    pyfuse3 only maps FUSEError to an errno; any other exception escaping a
    handler ends pyfuse3.main() and takes the whole mount down with it.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except pyfuse3.FUSEError:
            raise
        except FileNotFoundError as e:
            beetfs_logger.warning('beetfs: {}: source file missing: {}', func.__name__, e)
            raise pyfuse3.FUSEError(errno.ENOENT) from e
        except Exception as e: # pylint: disable=broad-exception-caught
            beetfs_logger.error('beetfs: {} failed: {!r}', func.__name__, e)
            raise pyfuse3.FUSEError(errno.EIO) from e
    return wrapper

def get_path_formats():
    """the path formats the tree is built from, as (query, format) pairs in order of
    precedence; the last one is the default format, with query None.

    beetfs.paths is laid out like beets' own paths, which are used when it isn't set.
    beetfs.path_format is shorthand for a beetfs.paths with only a default.
    """
    if config['beetfs']['paths'].exists():
        view = config['beetfs']['paths']
    elif config['beetfs']['path_format'].exists():
        return [(None, config['beetfs']['path_format'].as_str())]
    else:
        view = config['paths']
    formats, default = [], None
    for key, path_format in view.items():
        if key == 'default':
            default = path_format.as_str()
        else:
            query, _ = parse_query_string(PATH_FORMAT_QUERIES.get(key, key), Item)
            formats.append((query, path_format.as_str()))
    if default is None:
        raise UserError(f'{view.name} has no default path format')
    return formats + [(None, default)]

class Beetfs(beetsplugin):
    """plugin class for adding beetfs functionality to beets"""
    def commands(self):
        return [self.mount_command]

    def __init__(self):
        super().__init__()
        self.config.add({'art_dir': None})
        art_dir = self.config['art_dir'].get()
        if art_dir:
            redirect_album_art(art_dir)
        self.mount_command = subcommand('mount', help='mount a beets filesystem')
        self.mount_command.parser.set_usage(
            '%prog mount [mountpoint] [options]'
        )
        self.mount_command.parser.add_option(
            '-q',
            '--query',
            action='store',
            type='string',
            dest='query',
            help='query for filtering mounted items'
        )
        self.mount_command.func = self.mount

    def mount(self, lib, opts, args):
        """beetfs function for mounting the pyfuse3 filesystem"""
        if len(args) == 1: # mountpoint as arg
            mountpoints = {args[0]: ''}
        elif len(args) == 0: # pull mountpoints from config
            mountpoints = next(config['beetfs']['mounts'].resolve())[0]
        else:
            self.mount_command.parser.print_usage()
            return
        if opts.query:
            if len(args) != 1:
                self.mount_command.parser.print_usage()
                return
            mountpoints[args[0]] = opts.query
        path_formats = get_path_formats()
        fuse_options = set(pyfuse3.default_options)
        fuse_options.add('fsname=beetfs')
        fuse_options.add('allow_other')
        # the kernel then rejects every write (open for writing, setattr, create,
        # unlink, rename, ...) with EROFS before it reaches us
        fuse_options.add('ro')
        for mount, query in mountpoints.items():
            pid = os.fork()
            if pid == 0:
                try:
                    beetfs_operations = Operations(lib, mount, query, path_formats)
                    pyfuse3.init(beetfs_operations, mount, fuse_options)
                    trio.run(pyfuse3.main)
                except Exception as e:
                    print(f'Child failed for {mount}: {e}', flush=True)
                    pyfuse3.close()
                    raise
                pyfuse3.close()
                os._exit(0)

class Operations(pyfuse3.Operations): # pylint: disable=too-many-instance-attributes
    """class providing FUSE operations read, write, open, etc."""
    def __init__(self, library, mountpoint, query, path_formats):
        super().__init__()
        self.library = library
        self.path_formats = path_formats
        # each mount keeps its inode table in its own in-memory database so that
        # building it never touches (or write-locks) the beets library.db
        self.inode_db = sqlite3.connect(':memory:')
        self.build_inode_table(mountpoint, query)
        # keyed by item id: {'key', 'header_len', 'data_start', 'size'}
        self.header_info = {}
        # keyed by item id, least recently used first
        self.header_data = OrderedDict()
        # keyed by directory inode, least recently used first: (time found, art).
        # Holds image data, so it is bounded. Directories without art are
        # remembered separately: inode -> time checked.
        self.album_art_cache = OrderedDict()
        self.no_album_art = {}
        self.metadata_limiter = trio.CapacityLimiter(METADATA_THREADS)

    async def in_thread(self, func, *args):
        """runs blocking I/O in a worker thread; for quick calls on the audio read path"""
        return await trio.to_thread.run_sync(func, *args)

    async def in_metadata_thread(self, func, *args):
        """runs slow metadata work (header and art building) in the bounded metadata pool"""
        return await trio.to_thread.run_sync(func, *args, limiter=self.metadata_limiter)

    def inode_query(self, query, args=()):
        """runs a query against this mount's inode table"""
        beetfs_logger.debug('{} {}', query, args)
        return self.inode_db.execute(query, args).fetchall()

    def item_path(self, item):
        """the path components item is served at (the last one without extension),
        from the first path format whose query item matches"""
        path_format = next(t for q, t in self.path_formats if q is None or q.match(item))
        # for_path replaces any / within a field value, so only the format's own / split
        path = item.evaluate_template(path_format, True)
        components = [sanitize_path(x, self.library.replacements) for x in path.split('/')]
        # as in beets, an empty component is left out, so %if{$multidisc,$disc}/
        # only adds a directory level for albums with more than one disc
        return [x for x in components if x] or ['_']

    def entry_inode(self, parent, name, item, is_file):
        """the inode for a new entry: the file for item, or directory name in parent"""
        if is_file:
            return ITEM_INODE_FLAG | item.id
        # with 61 bits a collision is rare enough that which directory gets the next
        # candidate depending on build order doesn't matter
        for attempt in itertools.count():
            inode = directory_inode(parent, name, attempt)
            if inode > ROOT_INODE and not self.inode_db.execute(
                    'SELECT 1 FROM inodes WHERE inode = ?', (inode, )).fetchone():
                return inode
        raise AssertionError('unreachable')

    def add_entry(self, parent, name, item, ext=None):
        """adds a directory, or with ext the file for item, to the inode table.

        If another entry already has the name (e.g. a second edition of an album,
        or two discs that both have a "01 Intro"), both are kept, telling them apart
        by the beets id, which is stable across remounts. Returns (inode, name used).
        """
        is_file = ext is not None
        values = (item.id if is_file else None, item.album_id if is_file else None, item.added)
        insert = '''INSERT INTO inodes (inode, parent, name, item_id, album_id, item_added)
                    VALUES (?, ?, ?, ?, ?, ?)'''
        try:
            inode = self.entry_inode(parent, name, item, is_file)
            self.inode_db.execute(insert, (inode, parent, name + (ext or ''), *values))
        except sqlite3.IntegrityError:
            name += f' [{item.id}]'
            inode = self.entry_inode(parent, name, item, is_file)
            self.inode_db.execute(insert, (inode, parent, name + (ext or ''), *values))
        return inode, name

    def build_inode_table(self, mountpoint, query):
        """creates the inodes table from the items matching query"""
        beetfs_logger.info("Building inode tree for {}...", mountpoint)
        collisions = 0
        with self.inode_db:
            # item_id is NULL for a directory; album_id is kept so a directory of
            # tracks from different albums (e.g. singletons) gets no cover art.
            # The implicit rowid keeps the build order (beets' sort order), which
            # directories are listed in.
            self.inode_db.execute('''CREATE TABLE inodes (
                                         inode INTEGER NOT NULL UNIQUE,
                                         parent INTEGER,
                                         name TEXT,
                                         item_id INTEGER,
                                         album_id INTEGER,
                                         item_added REAL DEFAULT 0,
                                         UNIQUE (parent, name)
                                     );''')
            self.inode_db.execute("INSERT INTO inodes (inode, parent, name) VALUES (?, 0, '')",
                                  (ROOT_INODE, ))
            dirs = {} # (parent inode, name) -> inode
            dir_added = {ROOT_INODE: 0} # inode -> newest item_added in it
            for item in self.library.items(query=query):
                *dir_names, name = self.item_path(item)
                parent = ROOT_INODE
                dir_added[parent] = max(dir_added[parent], item.added)
                for dir_name in dir_names:
                    if (parent, dir_name) not in dirs:
                        dirs[(parent, dir_name)], _ = self.add_entry(parent, dir_name, item)
                    parent = dirs[(parent, dir_name)]
                    dir_added[parent] = max(dir_added.get(parent, 0), item.added)
                _, served_name = self.add_entry(parent, name, item, f'.{item.format.lower()}')
                if served_name != name:
                    collisions += 1
                    beetfs_logger.warning('beetfs: {} is already taken, serving item {} as {}',
                                          '/'.join(dir_names + [name]), item.id,
                                          '/'.join(dir_names + [served_name]))
            self.inode_db.executemany('UPDATE inodes SET item_added = ? WHERE inode = ?',
                                      [(added, inode) for inode, added in dir_added.items()])
        if collisions:
            beetfs_logger.warning('beetfs: {} items had the same path as another item; consider '
                                  'adding $disc and %aunique{{}} to the beetfs path formats', collisions)
        beetfs_logger.info("Done.")

    @staticmethod
    def art_from_file(path):
        """reads an image file as album art, or returns None if it can't be read"""
        try:
            with open(path, 'rb') as art_file:
                data = art_file.read()
            mtime_ns = os.stat(path).st_mtime_ns
        except OSError as e:
            beetfs_logger.debug('error reading cover file {}: {}', path, e)
            return None
        ext = os.path.splitext(os.fsdecode(path))[1].lower().replace('jpeg', 'jpg')
        return {'name': 'cover' + ext, 'data': data, 'mtime_ns': mtime_ns}

    def art_from_cover_file(self, source_dir):
        """looks for a cover art file next to the source audio files"""
        for cover_name in COVER_FILE_NAMES:
            cover_path = os.path.join(source_dir, cover_name)
            if os.path.exists(cover_path):
                art = self.art_from_file(cover_path)
                if art:
                    return art
        return None

    def art_from_beets(self, item):
        """the art beets has for item's album (album.artpath), e.g. from fetchart"""
        album = item.get_album() # None for singletons
        if album is None or not album.artpath:
            return None
        return self.art_from_file(album.artpath)

    def art_from_embedded(self, item):
        """extracts art embedded in an audio file"""
        try:
            data = MediaFile(os.fsdecode(item.path)).art
        except Exception as e: # pylint: disable=broad-exception-caught
            beetfs_logger.debug('error extracting embedded art from {}: {}', item.path, e)
            return None
        if not data:
            return None
        ext = '.png' if data.startswith(b'\x89PNG') else '.jpg'
        return {'name': 'cover' + ext, 'data': data,
                'mtime_ns': int(item.current_mtime() * 1e9)}

    def load_album_art(self, item_ids):
        """reads cover art for a directory's items; blocking, runs in a worker thread"""
        items = [self.library.get_item(item_id) for item_id in item_ids]
        items = [item for item in items if item is not None]
        art = None
        if items:
            art = (self.art_from_beets(items[0])
                   or self.art_from_cover_file(os.path.dirname(os.fsdecode(items[0].path))))
        if art is None:
            for item in items:
                art = self.art_from_embedded(item)
                if art:
                    break
        return art

    async def find_album_art(self, dir_inode):
        """finds cover art for a directory that directly contains items, cached per inode"""
        now = time.monotonic()
        checked = self.no_album_art.get(dir_inode)
        if checked is not None and now - checked < ALBUM_ART_RECHECK_SECONDS:
            return None
        if dir_inode in self.album_art_cache:
            found, art = self.album_art_cache[dir_inode]
            if now - found < ALBUM_ART_RECHECK_SECONDS:
                self.album_art_cache.move_to_end(dir_inode)
                return art
        art = None
        rows = self.inode_query('''SELECT item_id, album_id FROM inodes
                                   WHERE parent = ? AND item_id IS NOT NULL''', (dir_inode, ))
        # only a directory of one album's tracks, or of a single singleton
        albums = {album_id or ('singleton', item_id) for item_id, album_id in rows}
        if len(albums) == 1:
            art = await self.in_metadata_thread(self.load_album_art, [row[0] for row in rows])
        if art is None:
            self.no_album_art[dir_inode] = now
            self.album_art_cache.pop(dir_inode, None)
        else:
            self.no_album_art.pop(dir_inode, None)
            self.album_art_cache[dir_inode] = (now, art)
            self.album_art_cache.move_to_end(dir_inode)
            while len(self.album_art_cache) > ALBUM_ART_CACHE_SIZE:
                self.album_art_cache.popitem(last=False)
        return art

    async def album_art_getattr(self, inode):
        """builds attributes for a synthetic album-art inode"""
        art = await self.find_album_art(inode & ~ALBUM_ART_INODE_FLAG)
        if art is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        entry.st_mode = stat.S_IFREG | 0o444
        entry.st_nlink = 1
        entry.st_size = len(art['data'])
        entry.st_atime_ns = art['mtime_ns']
        entry.st_ctime_ns = art['mtime_ns']
        entry.st_mtime_ns = art['mtime_ns']
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_rdev = 0
        return entry

    async def beet_item_from_inode(self, inode):
        """fetches beets Item with inode"""
        rows = self.inode_query('SELECT item_id FROM inodes WHERE inode = ?', (inode, ))
        # beets may hold its database locked (e.g. during an import), so don't wait in the event loop
        item = await self.in_thread(self.library.get_item, rows[0][0]) if rows else None
        if item is None: # gone from the library since the mount was built
            raise pyfuse3.FUSEError(errno.ENOENT)
        return item

    def create_general_header(self, tags, filething):
        """creates a header using MediaFile write"""
        id3v23 = config["id3v23"].get(bool)
        mediafile = MediaFile(filething, id3v23=id3v23)
        mediafile.update(tags)
        filething.seek(0)
        mediafile.save()
        filething.seek(0)
        return filething.read()

    def create_header(self, item, tags, data_start):
        """rewrites the metadata before data_start in the source file with tags"""
        with open(item.path, 'rb') as f:
            source_header = f.read(data_start)
            # mutagen needs a few mp3 frames after the ID3 tag to recognise the file
            audio_frames = f.read(4096) if item.format == 'MP3' else b''
        header = self.create_general_header(tags, BytesIO(source_header + audio_frames))
        return header[:len(header) - len(audio_frames)] # remove fake frame data

    def find_mp3_data_start(self, item):
        """finds the offset in a source mp3 where the audio frames begin"""
        with open(item.path, 'rb') as bfile:
            beginning = bfile.read(3)
            if beginning == b'ID3': # There is ID3 tag info
                cursor = 6 # skip 'ID3', version, and flags
                bfile.seek(cursor)
                ssint = bfile.read(4)
                size = ssint[3] | ssint[2] << 7 | ssint[1] << 14 | ssint[0] << 21 # remove sync bits
                return size + 10
            if beginning[0] == 0xFF and beginning[1] & 0xE0 == 0xE0: # MPEG frame sync
                # beginning & 0xFFE000 == 0xFFE000, tfw working with bits in python
                return 0
            raise RuntimeError(f"What is this? {beginning}")

    def find_flac_data_start(self, item):
        """finds the offset in a source flac where the audio frames begin"""
        with open(item.path, 'rb') as bfile:
            cursor = 4 # first 4 bytes are 'fLaC'
            done = False
            while not done:
                bfile.seek(cursor)
                block_header = int(bfile.read(1).hex(), 16)
                length = int(bfile.read(3).hex(), 16)
                cursor += 4 + length
                done = block_header & 128 != 0
            return cursor

    def find_data_start(self, item):
        """chooses which data_start func to call based off of format reported by beets"""
        match item.format:
            case "FLAC":
                return self.find_flac_data_start(item)
            case "MP3":
                return self.find_mp3_data_start(item)
            case _:
                beetfs_logger.warning('File at {} is of unsupported filetype.', item.path)
                return None

    @staticmethod
    def source_key(item):
        """stats the source file and fingerprints the item's tags; blocking, runs in a
        worker thread. Returns (tags, stat, cache key)."""
        tags = {k: v for k, v in item.items() if k in item._media_fields}  # pylint: disable=protected-access
        source_stat = os.stat(item.path)
        return tags, source_stat, (source_stat.st_mtime_ns, source_stat.st_size, tags_digest(tags))

    def build_header(self, item, tags):
        """builds the header served for item; blocking, runs in a worker thread.
        Returns (data_start, header)."""
        try:
            data_start = self.find_data_start(item)
            if data_start is None: # unsupported format, serve the source file untouched
                return 0, b''
            return data_start, self.create_header(item, tags, data_start)
        except OSError:
            raise
        except Exception as e: # pylint: disable=broad-exception-caught
            # e.g. a FLAC with an ID3 tag in front of it: still playable, so serve it
            # untouched with its original tags rather than failing the request
            beetfs_logger.warning('beetfs: serving {} unmodified: {!r}', item.path, e)
            return 0, b''

    async def header_for(self, item, need_header=False):
        """returns (info, header, source stat) for the file served for item, rebuilding
        them when the source file or the item's tags have changed; header is only
        guaranteed when need_header is set.

        The cache dicts are only touched here, on the event loop thread; the blocking
        parts run in worker threads."""
        tags, source_stat, key = await self.in_thread(self.source_key, item)
        info = self.header_info.get(item.id)
        if info is not None and info['key'] == key:
            if not need_header:
                return info, None, source_stat
            if item.id in self.header_data:
                self.header_data.move_to_end(item.id)
                return info, self.header_data[item.id], source_stat
        data_start, header = await self.in_metadata_thread(self.build_header, item, tags)
        info = {'key': key, 'header_len': len(header), 'data_start': data_start,
                'size': len(header) + source_stat.st_size - data_start}
        self.header_info[item.id] = info
        self.header_data[item.id] = header
        self.header_data.move_to_end(item.id)
        while len(self.header_data) > HEADER_CACHE_SIZE:
            self.header_data.popitem(last=False)
        beetfs_logger.debug('built header for {}: {}', item.path,
                            {k: v for k, v in info.items() if k != 'key'})
        return info, header, source_stat

    @fuse_handler
    async def getattr(self, inode, ctx=None):
        """FUSE API implementation for 'get attributes'"""
        beetfs_logger.debug('getattr(self, {}, ctx={})', inode, ctx)
        if inode & ALBUM_ART_INODE_FLAG:
            return await self.album_art_getattr(inode)
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        rows = self.inode_query('SELECT item_id, item_added FROM inodes WHERE inode = ?', (inode, ))
        if not rows:
            raise pyfuse3.FUSEError(errno.ENOENT)
        item_id, item_added = rows[0]
        if item_id is None: # dir
            entry.st_mode = stat.S_IFDIR | 0o555
            entry.st_nlink = 2
            # these next entries should be more meaningful
            entry.st_size = 4096
            entry.st_atime_ns = 0
            entry.st_ctime_ns = 0
            entry.st_mtime_ns = int(float(item_added) * 1e9)
        else: # file
            item = await self.beet_item_from_inode(inode)
            info, _, source_stat = await self.header_for(item)
            entry.st_mode = stat.S_IFREG | 0o444
            entry.st_nlink = 1
            # the served file is the rewritten header plus the source audio
            entry.st_size = info['size']
            entry.st_atime_ns = source_stat.st_atime_ns
            entry.st_ctime_ns = source_stat.st_ctime_ns
            entry.st_mtime_ns = source_stat.st_mtime_ns
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_rdev = 0 # is this necessary?
        return entry

    @fuse_handler
    async def lookup(self, parent_inode, name, ctx=None):
        """FUSE API implementation for 'lookup'"""
        beetfs_logger.debug('lookup(self, {}, {}, {})', parent_inode, name, ctx)
        name = os.fsdecode(name)
        rows = self.inode_query('SELECT inode FROM inodes WHERE parent = ? AND name = ?',
                                (parent_inode, name))
        if rows:
            return await self.getattr(rows[0][0])
        if name.startswith('cover.'):
            art = await self.find_album_art(parent_inode)
            if art and name == art['name']:
                return await self.album_art_getattr(parent_inode | ALBUM_ART_INODE_FLAG)
        raise pyfuse3.FUSEError(errno.ENOENT)

    @fuse_handler
    async def opendir(self, inode, ctx):
        """FUSE API implementation for 'open directory'"""
        beetfs_logger.debug('opendir(self, {}, {})', inode, ctx)
        return inode

    # TODO() This does not yet do anything to preserve order or membership during a long readdir call
    #        See: https://pyfuse3.readthedocs.io/en/latest/fuse_api.html#pyfuse3.readdir_reply
    #        Potential fix would be some sort of mutex that allows for read-only access until all
    #        readdir_reply calls are finished
    @fuse_handler
    async def readdir(self, fh, start_id, token):
        """FUSE API implementation for 'read directory'"""
        beetfs_logger.debug('readdir(self, {}, {}, {})', fh, start_id, token)
        rows = self.inode_query('SELECT inode, name FROM inodes WHERE parent = ? ORDER BY rowid', (fh, ))
        for idx, (inode, name) in enumerate(rows):
            if idx < start_id:
                continue
            try:
                # also fails for an item removed from the library since the mount was built
                entry = await self.getattr(inode)
                # TODO() store the result here and manage lookup count
            except pyfuse3.FUSEError:
                continue # leave out the one broken entry rather than failing the whole listing
            if not pyfuse3.readdir_reply(token, os.fsencode(name), entry, idx + 1):
                return # token buffer full
        art_idx = len(rows)
        if start_id <= art_idx:
            art = await self.find_album_art(fh)
            if art:
                entry = await self.album_art_getattr(fh | ALBUM_ART_INODE_FLAG)
                pyfuse3.readdir_reply(token, bytes(art['name'], encoding='utf-8'), entry, art_idx + 1)

    @fuse_handler
    async def open(self, inode, flags, ctx):
        """FUSE API implementation for 'open file'"""
        beetfs_logger.debug('open(self, {}, {}, {})', inode, flags, ctx)
        # the ro mount option already stops these; kept in case it is mounted without it
        if flags & os.O_ACCMODE != os.O_RDONLY or flags & (os.O_TRUNC | os.O_APPEND):
            raise pyfuse3.FUSEError(errno.EROFS)
        if inode & ALBUM_ART_INODE_FLAG:
            if await self.find_album_art(inode & ~ALBUM_ART_INODE_FLAG) is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            return pyfuse3.FileInfo(fh=inode)
        # build the header now so the first read() doesn't have to
        await self.header_for(await self.beet_item_from_inode(inode), need_header=True)
        return pyfuse3.FileInfo(fh=inode)

    @fuse_handler
    async def read(self, fh, off, size):
        """FUSE API implementation for 'read file'"""
        beetfs_logger.debug('read(self, {}, {}, {})', fh, off, size)
        if fh & ALBUM_ART_INODE_FLAG:
            art = await self.find_album_art(fh & ~ALBUM_ART_INODE_FLAG)
            if art is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            return art['data'][off:off + size]
        item = await self.beet_item_from_inode(fh)
        info, header, _ = await self.header_for(item, need_header=True)
        data = header[off:off + size]
        if off + size > info['header_len']: # overlap into audio frames
            beetfs_logger.debug('data from {}', item.path)
            data += await self.in_thread(read_source, item.path,
                                         info['data_start'] + max(off - info['header_len'], 0),
                                         size - len(data))
        return data

    @fuse_handler
    async def release(self, fh):
        """FUSE API implementation for 'release file descriptor'"""
        beetfs_logger.debug('release(self, {})', fh)

    @fuse_handler
    async def flush(self, fh):
        """FUSE API implementation for 'flush to file'"""
        beetfs_logger.debug('flush(self, {})', fh)

    @fuse_handler
    async def statfs(self, ctx):
        """FUSE API implementation for 'get filesystem statistics'"""
        beetfs_logger.debug('statfs(self, {})', ctx)
        stats = pyfuse3.StatvfsData()
        stats.f_bsize = 4096
        stats.f_frsize = 4096
        stats.f_blocks = 1000000
        stats.f_bfree = 0 # read-only filesystem
        stats.f_bavail = 0
        stats.f_files = 1000000
        stats.f_ffree = 0
        stats.f_favail = 0
        return stats

    @fuse_handler
    async def access(self, inode, mode, ctx):
        """FUSE API implementation for 'check access permissions'"""
        beetfs_logger.debug('access(self, {}, {}, {})', inode, mode, ctx)
        if mode & os.W_OK: # read-only filesystem
            raise pyfuse3.FUSEError(errno.EROFS)
        return True

    @fuse_handler
    async def forget(self, inode_list):
        """FUSE API implementation for 'forget inodes'"""
        beetfs_logger.debug('forget(self, {})', inode_list)

    @fuse_handler
    async def getxattr(self, inode, name, ctx):
        """FUSE API implementation for 'get extended attribute'"""
        beetfs_logger.debug('getxattr(self, {}, {}, {})', inode, name, ctx)
        raise pyfuse3.FUSEError(errno.ENODATA)

    @fuse_handler
    async def listxattr(self, inode, ctx):
        """FUSE API implementation for 'list extended attributes'"""
        beetfs_logger.debug('listxattr(self, {}, {})', inode, ctx)
        return []
