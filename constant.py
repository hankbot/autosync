STATUS_IDLE   = 'idle'
STATUS_ACTIVE = 'active'
STATUS_PAUSED = 'paused'


RSYNC_COMMAND = "rsync"


# These are a bunch of constants that identify different type of file system
#  events and the fsevents library uses.
FS_EVENT_FLAG = {0x00000000: 'FlagNone', 0x00000001: 'MustScanSubDirs', 
           0x00000002: 'UserDropped', 0x00000004: 'KernelDropped',
           0x00000008: 'EventIdsWrapped', 0x00000010: 'HistoryDone', 
           0x00000020: 'RootChanged', 0x00000040: 'Mount', 
           0x00000080: 'Unmount', 0x00000100: 'ItemCreated',
           0x00000200:  'ItemRemoved', 0x00000400: 'ItemInodeMetaMod', 
           0x00000800: 'ItemRenamed', 0x00001000: 'ItemModified', 
           0x00002000: 'ItemFinderInfoMod',
           0x00004000: 'ItemChangeOwner', 0x00008000: 'ItemXattrMod', 
           0x00010000: 'ItemIsFile', 0x00020000: 'ItemIsDir', 
           0x00040000: 'ItemIsSymlink'}

LOG_FORMAT = '[%(asctime)-15s - %(levelname)s] %(module)s:%(lineno)s %(message)s '