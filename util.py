__version__ = "1.0"

from FSEvents import *
import objc
import os
import sys
import threading
#import signal
import logging
import hashlib
from Queue import Queue
from fsevents import Observer
from fsevents import Stream
from time import sleep
import gc

import constant as CONST
import config as CONFIG

import pprint

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
        
        self.sync_queue      = Queue(100000)
        self.sync_job_lock   = threading.Lock()
        self.dispatcher_lock = threading.Semaphore(0)
        
        # objc values
        self._oberver_runloop_ref = None
        
        # Start running
        self.sync_status = CONST.STATUS_IDLE
        
        self.init_job_thread()
        
        
    def init_job_thread(self):
        t = threading.Thread(target=self.job_dispatcher)
        t.daemon = True
        t.start()
        #self.job_thread = t
    
    def job_dispatcher(self):
        while True:
            logger.debug('Job disptcher run')
            
            #if self.sync_queue.qsize() % 10 or self.sync_queue.qsize() == 0:
                #gc.collect()
            logger.debug("qsize %s", self.sync_queue.qsize())
            self.dispatcher_lock.acquire()
            #if self.sync_queue.qsize() > 0:
            try:
                self.sync_job_lock.acquire()
                runner = job_runner(self.sync_queue)
                runner.run()
            finally:
                self.sync_job_lock.release()
                #t = threading.Thread(target=runner.run)
                #t.daemon = True
                #t.start()
    
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
        
        #self.start_observing_source()
        self.init_fsevent_observer()
        
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
    
    def fsevents_callback(streamRef, clientInfo, numEvents, eventPaths, eventMasks, eventIDs):
        logger.log("fsevents_callback")
    
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
        latency = 1.0
        flags   = 0
        
        fsevent_stream = FSEventStreamCreate(kCFAllocatorDefault, 
                                              self.process_fs_event,
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
    
    def process_fs_event(self, streamRef, clientInfo, numEvents, eventPaths, eventMasks, eventIDs):
        logger.debug("Process fs event - \n\
        clientInfo: %s, \n\
        numEvents: %s, \n\
        eventPaths: %s, \n\
        eventMasks: %s, \n\
        eventIDs: %s\n", clientInfo, numEvents, eventPaths, eventMasks, eventIDs)
        return
        events = []
        #gc.collect()
        
        for key, value in CONST.FS_EVENT_FLAG.iteritems():
            if mask & key:
                events.append(value)
        
        logger.debug("fsevent - File: %s, Event(s): %s",
                     path, ', '.join(events))
        logger.debug(os.path.abspath(path))
        
        job = self.create_job(path)

        if job:
            self.sync_queue.put(job)
            self.dispatcher_lock.release()
            logger.debug("Job was added to queue")
            logger.debug("Q size %i", self.sync_queue.qsize())
            logger.debug(job.source)
        else:
            logger.debug("Job was not added to queue %s", job)
            
        
        
    
    def create_job(self, event_path):
        
        source = event_path
        destination = ''
        
        job = sync_job(source, destination)
        
        return job
    
    def delete_job(self):
        pass
    

class sync_job(object):
        
    def __init__(self, source, destination):
        self.source      = source
        self.destination = destination
    
        self.depth = -1
        
        self.init_label()
        self.init_depth()
        
    def init_label(self):
        self.label = hashlib.md5(self.source +
                                 '|' + 
                                 self.destination).digest()
        
    def init_depth(self):
        pass
    
    
        
    

    
    
    
class job_runner(object):
    """ Runs the rsync command for the supplied job
    
    Execution blocks on the shared lock managed by cwsync
    
    """
    job_lock = threading.Lock()
    
    def __init__(self, job_queue):
        self.job_queue = job_queue
        logger.debug("Q size %i", self.job_queue.qsize())
    def run(self):
        try:
            #while 1:
            # rsync
            
            logger.debug("Aquiring Lock")
            #self.job_lock.acquire()
            #logger.debug("Job lock aquired")
            
            logger.debug("Looking for job")
            job = self.job_queue.get()
            logger.debug("Job got")
            logger.debug(job.source)
            

            
            logger.debug("Job started")
            logger.debug(job.source)
            #sleep(.5)
            logger.debug("Job completed")
        except Exception as e:
            logger.debug("Job threw exception")
            logger.debug(e)
        finally:
            self.job_queue.task_done()
            logger.debug("Job task done")
            #self.job_lock.release()
            #logger.debug("Job lock released")

            #logger.debug(gc.set_debug(gc.DEBUG_LEAK))
        
        
    
    