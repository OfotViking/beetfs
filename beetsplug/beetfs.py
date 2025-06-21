import os, stat, errno, datetime, pyfuse3, trio, logging, mimetypes
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC
from io import BytesIO
from beets import config
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand
from pathvalidate import sanitize_filename

if 'beetfs' in config:
    PATH_FORMAT = config['beetfs']['path_format'].get().split('/')
else:
    PATH_FORMAT = config['paths']['default'].get().split('/')

BEET_LOG = logging.getLogger('beets')
FLAC_PADDING = 2048 # 2KB padding

def mount(lib, opts, args):
    global library
    library = lib
    beetfs = Operations()
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=beetfs')
    fuse_options.add('allow_other')
    pyfuse3.init(beetfs, args[0], fuse_options)
    try:
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close()
        raise

    pyfuse3.close()

mount_command = subcommand('mount', help='mount a beets filesystem')
mount_command.func = mount

def get_id3_key(beet_key):
    key_map = {
        'album':                'album',
        'bpm':                  'bpm',
        #'':                    'compilation',
        'composer':             'composer',
        #'':                    'copyright',
        'encoder':              'encodedby',
        'lyricist':             'lyricist',
        'length':               'length',
        'media':                'media',
        #'':                    'mood',
        'title':                'title',
        #'':                    'version',
        'artist':               'artist',
        'albumartist':          'albumartist',
        #'':                    'conductor',
        'arranger':             'arranger',
        'disc':                 'discnumber',
        #'':                    'organization',
        'track':                'tracknumber',
        #'':                    'author',
        'albumartist_sort':     'albumartistsort',
        #'':                    'albumsort',
        'composer_sort':        'composersort',
        'artist_sort':          'artistsort',
        #'':                    'titlesort',
        #'':                    'isrc',
        #'':                    'discsubtitle',
        'language':             'language',
        'genre':                'genre',
        #'':                    'date',
        #'':                    'originaldate',
        #'':                    'performer:*',
        'mb_trackid':           'musicbrainz_trackid',
        #'':                    'website',
        'rg_track_gain':        'replaygain_*_gain',
        'rg_track_peak':        'replaygain_*_peak',
        'mb_artistid':          'musicbrainz_artistid',
        'mb_albumid':           'musicbrainz_albumid',
        'mb_albumartistid':     'musicbrainz_albumartistid',
        #'':                    'musicbrainz_trmid',
        #'':                    'musicip_puid',
        #'':                    'musicip_fingerprint',
        'albumstatus':          'musicbrainz_albumstatus',
        'albumtype':            'musicbrainz_albumtype',
        'country':              'releasecountry',
        #'':                    'musicbrainz_discid',
        'asin':                 'asin',
        #'':                    'performer',
        #'':                    'barcode',
        'catalognum':           'catalognumber',
        'mb_releasetrackid':    'musicbrainz_releasetrackid',
        'mb_releasegroupid':    'musicbrainz_releasegroupid',
        'mb_workid':            'musicbrainz_workid',
        'acoustid_fingerprint': 'acoustid_fingerprint',
        'acoustid_id':          'acoustid_id'
    }
    try:
        return key_map[beet_key]
    except KeyError:
        return None

class TreeNode():
    def find_type(self):
        if self.beet_item == None:
            return False
        path = self.beet_item.path
        filetype = mimetypes.guess_type(os.fsdecode(path))[0]
        BEET_LOG.debug("Filetype is " + str(filetype))
        return filetype

    def extract_album_art(self):
        """Extract album art from existing cover files or embedded in audio files"""
        if self.beet_id != -1:  # this is a file, not a directory
            return None
        
        # First, try to find existing cover art files in the source directory
        # Get the source directory from the first audio file
        source_dir = None
        for child in self.children:
            if child.beet_item:
                # Handle both string and bytes paths
                item_path = child.beet_item.path
                if isinstance(item_path, bytes):
                    item_path = os.fsdecode(item_path)
                source_dir = os.path.dirname(item_path)
                break
        
        if source_dir:
            # Common cover art file names
            cover_names = ['cover.jpg', 'cover.jpeg', 'cover.png', 'folder.jpg', 'folder.jpeg', 'folder.png',
                          'front.jpg', 'front.jpeg', 'front.png', 'album.jpg', 'album.jpeg', 'album.png']
            
            for cover_name in cover_names:
                cover_path = os.path.join(source_dir, cover_name)
                if os.path.exists(cover_path):
                    try:
                        with open(cover_path, 'rb') as cover_file:
                            cover_data = cover_file.read()
                            # Determine extension from file
                            _, ext = os.path.splitext(cover_name)
                            mime_type = 'image/jpeg' if ext.lower() in ['.jpg', '.jpeg'] else 'image/png'
                            BEET_LOG.debug(f"Found cover art file: {cover_path}")
                            return {
                                'data': cover_data,
                                'mime': mime_type,
                                'ext': ext.lower()
                            }
                    except Exception as e:
                        BEET_LOG.debug(f"Error reading cover file {cover_path}: {e}")
                        continue
            
        # If no external cover file found, try embedded album art
        for child in self.children:
            if child.beet_item and child.item_type in ['audio/mpeg', 'audio/flac']:
                try:
                    if child.item_type == 'audio/mpeg':
                        audio_file = MP3(child.beet_item.path)
                        if audio_file.tags:
                            for tag in audio_file.tags.values():
                                if isinstance(tag, APIC):
                                    BEET_LOG.debug(f"Found embedded album art in MP3: {child.beet_item.path}")
                                    return {
                                        'data': tag.data,
                                        'mime': tag.mime,
                                        'ext': '.jpg' if 'jpeg' in tag.mime.lower() else '.png'
                                    }
                    elif child.item_type == 'audio/flac':
                        audio_file = FLAC(child.beet_item.path)
                        if audio_file.pictures:
                            pic = audio_file.pictures[0]
                            BEET_LOG.debug(f"Found embedded album art in FLAC: {child.beet_item.path}")
                            return {
                                'data': pic.data,
                                'mime': pic.mime,
                                'ext': '.jpg' if 'jpeg' in pic.mime.lower() else '.png'
                            }
                except Exception as e:
                    BEET_LOG.debug(f"Error extracting album art from {child.beet_item.path}: {e}")
                    continue
        return None

    def find_mp3_data_start(self):
        if self.beet_item == None: # dir
            return False
        with open(self.beet_item.path, 'rb') as bfile:
            beginning = bfile.read(3)
            if beginning == b'ID3': # There is ID3 tag info
                cursor = 6 # skip 'ID3', version, and flags
                bfile.seek(cursor)
                ssint = bfile.read(4)
                size = ssint[3] | ssint[2] << 7 | ssint[1] << 14 | ssint[0] << 21 # remove sync bits
                return size + 10
            elif beginning[0] == 0xFF and beginning[1] & 0xE0 == 0xE0: # MPEG frame sync
                # beginning & 0xFFE000 == 0xFFE000, tfw working with bits in python
                return 0
            else:
                raise Exception('What is this? {}'.format(beginning))

    def find_flac_data_start(self):
        if self.beet_item == None: # dir
            return False
        with open(self.beet_item.path, 'rb') as bfile:
            cursor = 4 # first 4 bytes are 'fLaC'
            done = False
            while not done:
                bfile.seek(cursor)
                block_header = int(bfile.read(1).hex(), 16)
                length = int(bfile.read(3).hex(), 16)
                cursor += 4 + length
                done = block_header & 128 != 0
            return cursor

    def create_mp3_header(self):
        header = BytesIO()
        id3 = EasyID3()
        if self.beet_item == None: # dir
            return False
        for item in self.beet_item.items(): # beets tags
            key = get_id3_key(item[0])
            if item[1] and key:
                id3[key] = str(item[1])
        id3.save(fileobj=header, padding=(lambda x: 0))
        return header.getvalue()

    def create_flac_header(self): # should we do this with mutagen?
        if self.beet_item == None: # dir
            return False
        sections = {}
        with open(self.beet_item.path, 'rb') as bfile:
            cursor = 4 # first 4 bytes are 'fLaC'
            done = False
            while not done:
                bfile.seek(cursor)
                block_header_type = int(bfile.read(1).hex(), 16)
                length = int(bfile.read(3).hex(), 16)
                sections[block_header_type & 127] = bfile.read(length)
                cursor += 4 + length
                done = block_header_type & 128 != 0
        
        # Build vorbis comment with proper field count
        vorbis_comment = b''
        field_count = 0
        for item in self.beet_item.items():
            if item[1]:
                field_count += 1
                line = bytes(item[0].upper() + '=' + str(item[1]), 'utf-8')
                line = len(line).to_bytes(4, 'little') + line
                vorbis_comment += line
        
        # Prepend field count and vendor string
        vorbis_comment = field_count.to_bytes(4, 'little') + vorbis_comment
        vorbis_comment = b'\x05\x00\x00\x00beets' + vorbis_comment # 'beets' vendor string
        sections[4] = vorbis_comment # VORBIS_COMMENT
        
        # Build header, ensuring proper last-block flags
        header = b'fLaC' # beginning of flac header
        sorted_sections = sorted([s for s in sections.keys() if s != 1])  # exclude padding
        
        for i, section in enumerate(sorted_sections):
            is_last = (i == len(sorted_sections) - 1)  # last non-padding block
            block_type = section | (0x80 if is_last else 0x00)  # set last-block flag only for last block
            header += block_type.to_bytes(1, 'big') + len(sections[section]).to_bytes(3, 'big')
            header += bytes(sections[section])
        
        # Add padding as the final block with last-block flag
        header += b'\x81' + FLAC_PADDING.to_bytes(3, 'big')
        header += b'\x00' * FLAC_PADDING
        return header

    def __init__(self, name='', inode=1, beet_id=-1, mount_path='', parent=None, is_album_art=False):
        BEET_LOG.debug("Creating node " + str(name))
        self.name = name
        self.inode = inode
        self.beet_id = beet_id
        _beet_item = library.get_item(self.beet_id) if not is_album_art else None
        self.beet_item = None if not _beet_item else _beet_item
        self.mount_path = mount_path
        self.parent = parent
        self.children = []
        self.header = None
        self.is_album_art = is_album_art
        self.album_art_data = None
        
        if not is_album_art:
            self.item_type = self.find_type()
            self.data_start = self.find_mp3_data_start() if self.item_type == 'audio/mpeg' else self.find_flac_data_start() # where audio frame data starts in original file
            _header = self.create_mp3_header() if self.item_type == 'audio/mpeg' else self.create_flac_header()
            self.header_len = False if not _header else len(_header)
            if self.beet_item:
                self.size = self.header_len + os.path.getsize(self.beet_item.path) - self.data_start
        else:
            self.item_type = 'image/jpeg'  # assume JPEG for album art
            self.data_start = 0
            self.header_len = 0
            self.size = 0  # will be set when album art is loaded

    def add_child(self, child):
        for _child in self.children:
            if _child.name == child.name: # assumes unique names
                return _child
        self.children.append(child)
        return child

    def find(self, attr, target): # DFS
        BEET_LOG.debug("Searching for {} == {}".format(attr, target))
        if getattr(self, attr) == target:
            return self
        for child in self.children:
            result = child.find(attr, target)
            if result:
                return result
            
class beetfs(beetsplugin):
    def commands(self):
        return [mount_command]

class Operations(pyfuse3.Operations):
    enable_writeback_cache = True
    def __init__(self):
        super(Operations, self).__init__()
        self.next_inode = pyfuse3.ROOT_INODE + 1
        self.tree = self._build_fs_tree()

    def _build_fs_tree(self):
        items = list(library.items())
        root = TreeNode()
        height = len(PATH_FORMAT)
        for item in items:
            cursor = root
            for depth in range(0, height):
                name = sanitize_filename(item.evaluate_template(PATH_FORMAT[depth]))
                if depth == height - 1: # file
                    name += os.path.splitext(item.path)[-1].decode('utf-8') # add extension
                    beet_id = item.id
                else:
                    beet_id = -1
                mount_path = cursor.mount_path + '/' + name
                child = TreeNode(name, self.next_inode, beet_id, mount_path, cursor)
                cursor = cursor.add_child(child)
                if cursor.inode == self.next_inode: # if a new node was added to tree
                    self.next_inode += 1
        
        # Add album art files to directories
        self._add_album_art(root)
        return root
    
    def _add_album_art(self, node):
        """Recursively add album art files to directories that contain audio files"""
        if node.beet_id == -1:  # this is a directory
            # Check if this directory has any audio files
            has_audio = any(child.beet_item and child.item_type in ['audio/mpeg', 'audio/flac'] 
                          for child in node.children)
            
            if has_audio:
                # Try to extract album art
                art_data = node.extract_album_art()
                if art_data:
                    # Create cover.jpg file
                    cover_name = 'cover' + art_data['ext']
                    cover_path = node.mount_path + '/' + cover_name
                    cover_node = TreeNode(cover_name, self.next_inode, -1, cover_path, node, is_album_art=True)
                    cover_node.album_art_data = art_data['data']
                    cover_node.size = len(art_data['data'])
                    node.add_child(cover_node)
                    self.next_inode += 1
        
        # Recursively process children
        for child in node.children:
            self._add_album_art(child)

    async def getattr(self, inode, ctc=None):
        BEET_LOG.debug('getattr(self, {}, ctc={})'.format(inode, ctc))
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        item = self.tree.find('inode', inode)
        if item.beet_id == -1 and not item.is_album_art: # dir
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_nlink = 2
            # these next entries should be more meaningful
            entry.st_size = 4096
            entry.st_atime_ns = 0
            entry.st_ctime_ns = 0
            entry.st_mtime_ns = 0
        else: # file (audio or album art)
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_nlink = 1
            entry.st_size = item.size
            if item.is_album_art:
                # Use current time for album art files
                current_time = datetime.datetime.now().timestamp() * 1e9
                entry.st_atime_ns = current_time
                entry.st_ctime_ns = current_time
                entry.st_mtime_ns = current_time
            else:
                entry.st_atime_ns = os.path.getatime(item.beet_item.path) * 1e9
                entry.st_ctime_ns = os.path.getctime(item.beet_item.path) * 1e9
                entry.st_mtime_ns = os.path.getmtime(item.beet_item.path) * 1e9
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_rdev = 0 # is this necessary?
        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        BEET_LOG.debug('lookup(self, {}, {}, {})'.format(parent_inode, name, ctx))
        item = self.tree.find('inode', parent_inode)
        for child in item.children:
            if child.name == name:
                return self.getattr(child.inode)
        ret = pyfuse3.EntryAttributes()
        ret.st_ino = 0
        return ret

    async def opendir(self, inode, ctx):
        BEET_LOG.debug('opendir(self, {}, {})'.format(inode, ctx))
        return inode

    async def readdir(self, inode, start_id, token):
        BEET_LOG.debug('readdir(self, {}, {}, {})'.format(inode, start_id, token))
        if start_id == 0: # only need to read once to get DB values
            item = self.tree.find('inode', inode)
            for child in item.children:
                entry = await self.getattr(child.inode)
                pyfuse3.readdir_reply(token, bytes(child.name, encoding='utf-8'), entry, start_id + 1)
        return

    async def open(self, inode, flags, ctx):
        BEET_LOG.debug('open(self, {}, {}, {})'.format(inode, flags, ctx))
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        item = self.tree.find('inode', inode)
        BEET_LOG.debug('open: item_type={}'.format(item.item_type))
        if not item.is_album_art:
            item.header = item.create_mp3_header() if item.item_type == 'audio/mpeg' else item.create_flac_header()
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        BEET_LOG.debug('read(self, {}, {}, {})'.format(fh, off, size))
        item = self.tree.find('inode', fh) # fh = inode
        
        if item.is_album_art:
            # Handle album art file reading
            if off >= len(item.album_art_data):
                return b''
            return item.album_art_data[off:off + size]
        
        # Handle audio file reading with custom headers
        data = b''
        if off <= item.header_len:
            data += item.header[off:off + size]
            if off + size > item.header_len:
                size = size - (item.header_len - off) # overlap into audio frames
                off = item.header_len
            else:
                return data
        BEET_LOG.debug('data from {}'.format(item.beet_item.path))
        with open(item.beet_item.path, 'rb') as bfile:
            data_off = off - item.header_len + item.data_start
            bfile.seek(data_off)
            data += bfile.read(size)
        return data

    async def release(self, fh):
        BEET_LOG.debug('release(self, {})'.format(fh))
        item = self.tree.find('inode', fh) # fh = inode
        item.header = None # to prevent holding headers in memory

    async def flush(self, fh):
        BEET_LOG.debug('flush(self, {})'.format(fh))
