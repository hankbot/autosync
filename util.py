__version__ = "1.0"

from FSEvents import *
from Queue import Queue
from fsevents import Observer, Stream
from time import sleep
import config as CONFIG
import constant as CONST
import gc
import hashlib
import logging
import objc
import os
import pprint
import sys
import string
import threading
from tempfile import NamedTemporaryFile 
from subprocess import call
#import signal



logging.basicConfig(format=CONST.LOG_FORMAT)

logger = logging.getLogger('utl')
logger.setLevel(CONFIG.LOG_LEVEL)

gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_UNCOLLECTABLE)

class fsevent_sync(object):
    
    def __init__(self):
    
        self.sync_source      = None
        self.sync_destination = None
        
        self.fs_stream   = None
        self.fs_observer = None
        
        self.job_thread = None
        
        self.sync_job_lock   = threading.Lock()
        self.dispatcher_lock = threading.Condition()
        self.event_path_list = []
        
        # objc values
        self._oberver_runloop_ref = None
        
        # Start running
        self.sync_status = CONST.STATUS_IDLE
        
        self.init_job_thread()
        
        
    def init_job_thread(self):
        t = threading.Thread(target=self.job_dispatcher)
        t.daemon = True
        t.start()
        self.job_thread = t
    
    def job_dispatcher(self):
        ''' Waits for signalling from Process the '''
        sync_queue = Queue()
        
        while True:
            
            #in each dispatcher iteration 
            self.dispatcher_lock.acquire()
            
            if not len(self.event_path_list):
                # wait for fsevent(s) to come in
                gc.collect() # maybe not necessary
                self.dispatcher_lock.wait()
            
            # create and queue a job for the current list of paths
            job = self.create_job(self.event_path_list)
                
            if job:
                sync_queue.put(job)
            else:
                logger.debug("Job was not added to queue %s", job)
            
            del job
            
            # clear list after processing
            self.event_path_list = []
            
            # release lock to allow fsevents to add to self.event_path_list
            self.dispatcher_lock.release()
            
            # execute all jobs currently in the sync_queue
            # rsync jobs could compete, run one at a time
            while sync_queue.qsize() > 0:
                try:
                    runner = job_runner(sync_queue.get())
                    t = threading.Thread(target=runner.run)
                    t.daemon = True
                    t.start()
                    t.join()
                    del t
                except Exception as e:
                    logger.debug('sync job threw exception')
                    logger.debug(e)
                finally:
                    sync_queue.task_done()
    
    def set_sync_source(self, source):
        path = os.path.abspath(source)
        logger.debug(path)
        if self.validate_source(source):
            self.sync_source = path
            logger.debug("sync_source set: %s", self.sync_source)
            return True
        self.sync_source = None
        logger.debug("sync_source NOT set: %s", self.sync_source)
        return False
    
    def set_sync_destination(self, destination):
        #self.validate_destination(destination)
        #validate to be local path or host:path
        self.sync_destination = destination
        return True
    
    def validate_source(self, source):
        if not os.path.isdir(source):
            logger.debug("Source is not a directory %s", source)
            return False
        
        if not os.access(source, os.R_OK):
            logger.debug("Could not read source directory %s", source)
            return False
        
        return True
    
    def validate_destination(self):
        # determine if local or ssh path
        # if local validate dir existence and perms
        # if remote check for form
        return True
    
    def start_sync(self):
        """Create a fsevent observer and start watching the source"""
        #@todo: self.validate_source(source)
        #@todo: self.validate_destination(destination)
        
        if self.sync_source == None:
            logger.debug('Could not start sync, sync_source eq None')
            return False
        
        self.start_observing_source()
        
        self.sync_stats = CONST.STATUS_ACTIVE
    
    def pause_sync(self):
        # kill rsync command? In separate process?
        # requeue current rsync command
        # preserve fs events
        # preserve jobs
        logger.debug('pausing')
        
        self.stop_observing_source()
        self.sync_status = CONST.STATUS_IDLE
        logger.debug('paused')
        
        
    def stop_sync(self):
        # kill rsync command? In separate process?
        # clear fs events
        # clear jobs
        self.stop_observing_source()
        self.sync_status = CONST.STATUS_IDLE
        
    def start_observing_source(self):
        t = threading.Thread(target=self.init_fsevent_observer)
        t.daemon = True
        t.start()
    
    def stop_observing_source(self):
        
        logger.debug('Stop observing')
        
        if not self._oberver_runloop_ref == None:
            CFRunLoopStop(self._oberver_runloop_ref)
            logger.debug('CFRunLoop stopped')
            
        logger.debug('CFRunLoop')
    
    def init_fsevent_observer(self):
        ''' Instantiate and run an FSEventStream in a CFRunLoop. 
        
        Intended to be used in a separate thread to asynchronously report 
        fsevents using the self.process_fs_event callback
        
        '''
        
        pool = NSAutoreleasePool.alloc().init()
        
        '''
        FSEventStreamCreate
        extern FSEventStreamRef FSEventStreamCreate(
           CFAllocatorRef allocator,
           FSEventStreamCallback callback,
           FSEventStreamContext *context,
           CFArrayRef pathsToWatch,
           FSEventStreamEventId sinceWhen,
           CFTimeInterval latency,
           FSEventStreamCreateFlags flags);
        '''
        
        since   = -1
        latency = 4.0
        flags   = 0
        
        fsevent_stream = FSEventStreamCreate(kCFAllocatorDefault, 
                                              self.process_fsevent,
                                              self.sync_source,
                                              [self.sync_source],
                                              since,
                                              latency,
                                              flags)

        FSEventStreamScheduleWithRunLoop(fsevent_stream, 
                                         CFRunLoopGetCurrent(), 
                                         kCFRunLoopDefaultMode)
        
        stream_started = FSEventStreamStart(fsevent_stream)
        if not stream_started:
            logger.error( "Failed to start the FSEventStream")
            return
        
        # keep a reference to the loop so it can be stopped e.g. pause, stop
        self._oberver_runloop_ref = CFRunLoopGetCurrent()
        
        try:
            logger.debug("Start EVENT LOOP")
            CFRunLoopRun()
            logger.debug("EVENT LOOP Stopped")
        finally:
            # Clean up stream and event loop
            logger.debug("EVENT LOOP finally")
            FSEventStreamStop(fsevent_stream)
            FSEventStreamInvalidate(fsevent_stream)
            del pool
    
    def process_fsevent(self, stream_ref, client_info, event_count, event_paths, 
                        event_masks, event_ids):
        logger.debug("Process fs event - \n\
        client_info: %s, \n\
        event_count: %s, \n\
        event_paths: %s, \n\
        event_masks: %s, \n\
        event_ids: %s\n", client_info, event_count, event_paths, event_masks, 
        event_ids)
        
        # lock access to the event list
        self.dispatcher_lock.acquire()
        
        # add the new event paths to the list
        for i in range(event_count):
            self.event_path_list.append(event_paths[i])
        
        # increase the dispatcher semaphore, starts event path processing and 
        #    job creation
        logger.debug("fsevent notify dispatcher")
        self.dispatcher_lock.notify()
        logger.debug("fsevent release dispatcher")
        self.dispatcher_lock.release()
        
    
    def create_job(self, job_paths):
        
        abs_job_paths = []
        
        # process soruces
        for p in job_paths:
            abs_job_paths.append(os.path.abspath(p))
         
        abs_source      = os.path.abspath(self.sync_source)
        abs_destination = os.path.abspath(self.sync_destination)
        
        job = sync_job(abs_source, abs_destination, abs_job_paths)
        
        return job
    
    def delete_job(self):
        pass
    

class sync_job(object):
        
    def __init__(self, source, destination, job_paths):
        self.source      = source
        self.destination = destination
        self.job_paths   = job_paths
    
class job_runner(object):
    """ Runs the rsync command for the supplied job
    
    Execution blocks on the shared lock managed by cwsync
    
    """
    
    #job_lock = threading.Lock()
    
    def __init__(self, job):
        self.job = job
        
    def run(self):
        try:
            
            logger.debug(self.job.source)
            
            logger.debug("Job started")
            
            files_from_file = self.write_rysnc_include_file()
            self.rsync(files_from_file.name)
            #logger.debug(files_from_file.read())
            
            #
            
            logger.debug("Job completed")
        except Exception as e:
            logger.debug("Job threw exception")
            logger.debug(e)
        finally:
            # close and cleanup tmp file
            files_from_file.close()

        
    def write_rysnc_include_file(self):
        ''' Writes out a file to be passed to the rsync --files-from option.
        An open file reference is retuned with the pointed set at the start of
        the file'''
        
        f = NamedTemporaryFile()
        logger.debug('self.job.job_paths: %s', self.job.job_paths)
        for p in self.job.job_paths:
            rp = string.replace(p, self.job.source, '')
            
            if rp == '':
                rp ='.'
            
            f.write(rp+"/\n")
        
        f.seek(0)
        return f
    
    def rsync(self, include_filename):
        #rsync -v -dltu --delete  --files-from="/tmp/from" /Users/mkiedrowski/cwsync_test/rsync/source/ /Users/mkiedrowski/cwsync_test/rsync/dest/
        #call(['rsync', '--delete', '-dltu', self.job.source+'/', self.job.destination+'/'], shell=True)
        #call(['rsync --delete -rltu --files-from="'+include_filename+'" '+self.job.source+'/ '+self.job.destination+'/'], shell=True)
        call(['rsync --delete -rltu '+self.job.source+'/ '+self.job.destination+'/'], shell=True)
    