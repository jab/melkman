
def test_many_readers():
    from melkman.green import RWLock, timeout_wait
    from eventlet.proc import spawn
    from eventlet.coros import event

    rwlock = RWLock()

    ready1 = event()
    ready2 = event()

    def reader1():
        rwlock.read_acquire()
        ready1.send(True)
        
    def reader2():
        rwlock.read_acquire()
        ready2.send(True)

    spawn(reader1)
    spawn(reader2)
        
    timeout_wait(ready1, .5)
    timeout_wait(ready2, .5)

    assert ready1.has_result()
    assert ready2.has_result()

def test_single_writer():
    from melkman.green import RWLock, timeout_wait
    from eventlet.proc import spawn
    from eventlet.coros import event

    rwlock = RWLock()

    ready1 = event()
    ready2 = event()
    resume = event()

    def writer1():
        rwlock.write_acquire()
        ready1.send(True)
        resume.wait()
        rwlock.write_release()

    def writer2():
        rwlock.write_acquire()
        ready2.send(True)
        resume.wait()
        rwlock.write_release()

    spawn(writer1)
    spawn(writer2)
        
    timeout_wait(ready1, .5)
    timeout_wait(ready2, .5)

    writers = 0
    if ready1.has_result():
        writers += 1
    if ready2.has_result():
        writers += 1
    
    assert writers == 1, 'Expected 1 writer, got %d' % writers

    resume.send(True)

    timeout_wait(ready1, .5)
    timeout_wait(ready2, .5)

    writers = 0
    if ready1.has_result():
        writers += 1
    if ready2.has_result():
        writers += 1
    
    assert writers == 2, 'Expected 2 writers, got %d' % writers

    
def test_write_excludes_read():
    from melkman.green import RWLock, timeout_wait
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from eventlet.coros import event

    rwlock = RWLock()

    writer_has_lock = event()
    reader_has_lock = event()

    release_write_lock = event()
    def writer():
        rwlock.write_acquire()
        writer_has_lock.send(True)
        release_write_lock.wait()
        rwlock.write_release()

    def reader():
        writer_has_lock.wait()
        rwlock.read_acquire()
        reader_has_lock.send(True)
        rwlock.read_release()
        
    spawn(writer)
    spawn(reader)
    
    timeout_wait(writer_has_lock, .5)
    sleep(.3)

    assert writer_has_lock.has_result()
    assert not reader_has_lock.has_result()

    release_write_lock.send(True)
    timeout_wait(reader_has_lock, .5)
    assert reader_has_lock.has_result()
    

def test_read_excludes_write():
    from melkman.green import RWLock, timeout_wait
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from eventlet.coros import event

    rwlock = RWLock()

    writer_has_lock = event()
    reader_has_lock = event()

    def writer():
        reader_has_lock.wait()
        rwlock.write_acquire()
        writer_has_lock.send(True)
        rwlock.write_release()

    release_read_lock = event()
    def reader():
        rwlock.read_acquire()
        reader_has_lock.send(True)
        release_read_lock.wait()
        rwlock.read_release()

    spawn(writer)
    spawn(reader)
    
    timeout_wait(reader_has_lock, .5)
    sleep(.3)

    assert not writer_has_lock.has_result()
    assert reader_has_lock.has_result()

    release_read_lock.send(True)
    timeout_wait(writer_has_lock, .5)
    
    assert writer_has_lock.has_result()
    
def test_write_priority():
    from melkman.green import RWLock, timeout_wait
    from eventlet.api import sleep
    from eventlet.coros import event
    from eventlet.proc import spawn

    rwlock = RWLock()
    
    reader_has_lock = event()
    writer1_has_lock = event()
    writer2_has_lock = event()

    writer1_release = event()
    writer2_release = event()
    def writer1():
        rwlock.write_acquire()
        writer1_has_lock.send(True)
        writer1_release.wait()
        rwlock.write_release()

    def reader():
        writer1_has_lock.wait()
        rwlock.read_acquire()
        reader_has_lock.send(True)

    def writer2():
        rwlock.write_acquire()
        writer2_has_lock.send(True)
        writer2_release.wait()
        rwlock.write_release()

    spawn(writer1)
    spawn(reader)
    
    timeout_wait(writer1_has_lock, .5)
    timeout_wait(reader_has_lock, .2) # should timeout..
    assert writer1_has_lock.has_result()
    assert not reader_has_lock.has_result()
    
    spawn(writer2)
    sleep(.2)
    
    # writer 2 should take precedence over the reader
    # although it started later and asked for the lock
    # later.
    writer1_release.send(True)
    timeout_wait(writer2_has_lock, .5)
    timeout_wait(reader_has_lock, .2) # should timeout..
    assert writer2_has_lock.has_result()
    assert not reader_has_lock.has_result()

    writer2_release.send(True)
    timeout_wait(reader_has_lock, .5)
    assert reader_has_lock.has_result()