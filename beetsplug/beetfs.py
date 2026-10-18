"""Module providing the beetfs plugin for beets"""

from collections import OrderedDict
from io import BytesIO
import errno
import functools
import hashlib
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
from beets.library import Album
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand
from beets.util import bytestring_path, normpath, sanitize_path

beetfs_logger = logging.getLogger('beets')

# bit flag marking synthetic album-art inodes; real inodes come from the
# sqlite table and stay far below this
ALBUM_ART_INODE_FLAG = 1 << 40

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

def get_path_format():
    """fetches the path format from config else gives default value"""
    if 'beetfs' in config:
        return config['beetfs']['path_format'].get().split('/')
    return config['paths']['default'].get().split('/')

def quote_ident(name):
    """quotes an SQL identifier such as a path format column"""
    return '"' + name.replace('"', '""') + '"'

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
                    beetfs_operations = Operations(lib, mount, query)
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
    def __init__(self, library, mountpoint, query):
        super().__init__()
        self.library = library
        self.path_format = get_path_format()
        self.columns = ', '.join([quote_ident(x) for x in self.path_format])
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

    def build_inode_table(self, mountpoint, query):
        """creates the inodes table from the items matching query"""
        beetfs_logger.info("Building inode tree for {}...", mountpoint)
        column_defs = ', '.join([f'{quote_ident(x)} TEXT' for x in self.path_format])
        insert_item = f'''INSERT INTO inodes ({self.columns}, item_id, item_added)
                          VALUES ({', '.join(['?'] * (len(self.path_format) + 2))})'''
        # directories are shared by many items: inserting an existing one just
        # updates its timestamp
        insert = insert_item + f''' ON CONFLICT ({self.columns})
                     DO UPDATE SET item_added = MAX(excluded.item_added, item_added)'''
        collisions = 0
        with self.inode_db:
            self.inode_db.execute(f'''CREATE TABLE inodes (
                                          inode INTEGER PRIMARY KEY,
                                          {column_defs},
                                          item_id INTEGER,
                                          item_added REAL DEFAULT 0,
                                          UNIQUE ({self.columns})
                                      );''')
            # insert a blank entry for the root inode "1"
            # use '' instead of NULL to enable SQL UNIQUE
            self.inode_db.execute(insert, [''] * len(self.path_format) + ['', 0])
            for item in self.library.items(query=query):
                # replace / with _ for unix path sanity
                components = [
                    # an empty component would make the row look like its parent directory
                    sanitize_path(item.evaluate_template(x, True), self.library.replacements) or '_'
                    for x in self.path_format
                ]
                for depth in range(1, len(components)):
                    values = components[:depth] + [''] * (len(components) - depth)
                    values += ['', item.added]
                    self.inode_db.execute(insert, values)
                try:
                    self.inode_db.execute(insert_item, components + [item.id, item.added])
                except sqlite3.IntegrityError:
                    # another item already has this path (e.g. a second edition of an
                    # album, or two discs that both have a "01 Intro"). Keep both,
                    # telling them apart by the beets id, which is stable across remounts.
                    collisions += 1
                    original = '/'.join(components)
                    components[-1] += f' [{item.id}]'
                    self.inode_db.execute(insert_item, components + [item.id, item.added])
                    beetfs_logger.warning('beetfs: {} is already taken, serving item {} as {}',
                                          original, item.id, '/'.join(components))
            # update root inode
            self.inode_db.execute('''UPDATE inodes SET item_added = (SELECT MAX(item_added)
                                     FROM inodes WHERE inode != 1) WHERE inode = 1;''')
        if collisions:
            beetfs_logger.warning('beetfs: {} items had the same path as another item; consider '
                                  'adding $disc and %aunique{{}} to beetfs.path_format', collisions)
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
        rows = self.inode_query(f'SELECT {self.columns} FROM inodes WHERE inode = ?', (dir_inode, ))
        result = [x for x in rows[0] if x != ''] if rows else []
        if rows and len(result) == len(self.path_format) - 1: # directory of items
            where_clause = ' AND '.join([f'{quote_ident(x)} = ?' for x in self.path_format[:len(result)]])
            query = f'SELECT item_id FROM inodes WHERE {where_clause} AND item_id != ?'
            rows = self.inode_query(query, tuple(result + ['']))
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
        rows = self.inode_query(f'SELECT {self.columns}, item_added FROM inodes WHERE inode = ?', (inode, ))
        if not rows:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if len([x for x in rows[0][:-1] if x != '']) < len(self.path_format): # dir
            entry.st_mode = stat.S_IFDIR | 0o555
            entry.st_nlink = 2
            # these next entries should be more meaningful
            entry.st_size = 4096
            entry.st_atime_ns = 0
            entry.st_ctime_ns = 0
            entry.st_mtime_ns = int(float(rows[0][-1]) * 1e9)
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
        rows = self.inode_query(f'SELECT {self.columns} FROM inodes WHERE inode = ?', (parent_inode, ))
        if not rows:
            raise pyfuse3.FUSEError(errno.ENOENT)
        result = [x for x in rows[0] if x != '']
        depth = len(result)
        if depth == len(self.path_format) - 1: # children are beet items
            art = await self.find_album_art(parent_inode)
            if art and name == art['name']:
                return await self.album_art_getattr(parent_inode | ALBUM_ART_INODE_FLAG)
            name = os.path.splitext(name)[0] # stored names have no extension
        values = result + [name] + [''] * (len(self.path_format) - depth - 1)
        where_clause = ' AND '.join([f'{quote_ident(x)} = ?' for x in self.path_format])
        rows = self.inode_query(f'SELECT inode FROM inodes WHERE {where_clause}', tuple(values))
        if not rows:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return await self.getattr(rows[0][0])

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
        rows = self.inode_query(f'SELECT {self.columns} FROM inodes WHERE inode = ?', (fh, ))
        result = [x for x in rows[0] if x != '']
        depth = len(result)
        fixed_columns = self.path_format[:depth] + self.path_format[depth+1:]
        col = quote_ident(self.path_format[depth])
        where_clause = ' AND '.join([f'{quote_ident(x)} = ?' for x in fixed_columns] + [f'{col} != ?'])
        query = f'SELECT inode, item_id, {col} FROM inodes WHERE {where_clause}'
        values = result + [''] * (len(self.path_format) - depth - 1) + ['']
        rows = self.inode_query(query, tuple(values))
        for idx, row in enumerate(rows):
            if idx < start_id:
                continue
            try:
                entry = await self.getattr(row[0])
                # TODO() store the result here and manage lookup count
                reply_name = row[2]
                if depth == len(self.path_format)-1: # beet item
                    item = await self.in_thread(self.library.get_item, row[1])
                    if item is None: # removed from the library since the mount was built
                        continue
                    reply_name += f'.{item.format.lower()}'
            except pyfuse3.FUSEError:
                continue # leave out the one broken entry rather than failing the whole listing
            if not pyfuse3.readdir_reply(token, bytes(reply_name, encoding='utf-8'), entry, idx + 1):
                return # token buffer full
        art_idx = len(rows)
        if depth == len(self.path_format)-1 and start_id <= art_idx: # directory of beet items
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
