

active_capture = None

class BuildCapture:
    pass

def init_capture():
    global active_capture
    if active_capture is None:
        print "none"
    active_capture = BuildCapture()
    return active_capture
