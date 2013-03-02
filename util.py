__version__ = "1.0"

from FSEvents import *
from Queue import Queue
import config as CONFIG
import constant as CONST
import gc
import logging
import os
import pprint
import string
import threading
from subprocess import call

logging.basicConfig(format=CONST.LOG_FORMAT)

logger = logging.getLogger('utl')
logger.setLevel(CONFIG.LOG_LEVEL)

#gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_UNCOLLECTABLE)

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
        
        
        self.sync_status = CONST.STATUS_IDLE
        
        # Start running
        self.init_job_thread()
        
    def __del__(self):
        CFRunLoopStop(self._oberver_runloop_ref)
        
    def init_job_thread(self):
        t = threading.Thread(target=self.job_dispatcher)
        t.daemon = True
        t.start()
        self.job_thread = t
    
    def job_dispatcher(self):
        ''' Creates and runs sync_jobs as fsevents are reported 
        
        '''
        
        sync_queue = Queue()
        
        while True:
            
            #in each dispatcher iteration 
            self.dispatcher_lock.acquire()
            
            if not len(self.event_path_list):
                # wait for fsevent(s) to come in
                gc.collect() # maybe not necessary
                logger.info('Waiting for changes')
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
                    logger.info('sync started...')
                    t.start()
                    t.join()
                    del t
                except Exception as e:
                    logger.debug(e)
                    logger.info('There were errors during the sync')
                finally:
                    sync_queue.task_done()
                    logger.info('sync complete')
    
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
        
        self.sync_status = CONST.STATUS_ACTIVE
    
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
        
        if self._oberver_runloop_ref == None:
            t = threading.Thread(target=self.init_fsevent_observer)
            t.daemon = True
            t.start()
        else:
            logger.debug('CFRunLoop is running, will not start another')
    
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
            CFRunLoopRun()
        finally:
            # Clean up stream and event loop
            FSEventStreamStop(fsevent_stream)
            FSEventStreamInvalidate(fsevent_stream)
            del pool
            self._oberver_runloop_ref = None
    
    def process_fsevent(self, stream_ref, client_info, event_count, event_paths, 
                        event_masks, event_ids):
        ''' Used as the callback when fsevents are reported.
        Originally the reported events paths were intended to be used to 
        optimize the rsync command, however this was unreliable and the 
        most consistent results were achieved by always calling rsync with the
        top level source only.
        
        The event list is still used processed at the subpath level to aid in 
        debugging when needed.
        
        '''
        
        # lock access to the event list
        self.dispatcher_lock.acquire()
        
        # add the new event paths to the list
        for i in range(event_count):
            self.event_path_list.append(event_paths[i])
        
        # signal to start event path processing and job creation
        self.dispatcher_lock.notify()
        self.dispatcher_lock.release()
        
    def create_job(self, job_paths):
        
        abs_job_paths = []
        
        # process soruces
        for p in job_paths:
            abs_job_paths.append(os.path.abspath(p))
         
        _source      = os.path.abspath(self.sync_source)
        
        # if it looks like a local path, get abs
        if os.path.exists(os.path.abspath(self.sync_destination)):
            _destination = os.path.abspath(self.sync_destination)
            logger.debug('Local Job: %s', _destination)
        elif string.find(self.sync_destination, ':') > 0:
            _destination = self.sync_destination
            logger.debug('Remote Job: %s', _destination)
        else:
            return
        
        job = sync_job(_source, _destination, abs_job_paths)
        
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
    
    def __init__(self, job):
        self.job = job
        
    def run(self):
        try:
            logger.debug('start rsync')
            call(['rsync --delete -rltu '+self.job.source+'/ '+self.job.destination+'/'], shell=True)
            logger.debug('rsync complete')
        except Exception as e:
            logger.debug(e)
    