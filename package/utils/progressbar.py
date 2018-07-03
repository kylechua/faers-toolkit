import sys

def update(message, progress):
    barLength = 10 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "\x1b[6;37;42mFinished!\x1b[0m\r\n"
    block = int(round(barLength*progress))
    text = ("\r" + message + ": {0} {1}% {2}").format( "\x1b[1;35;44m" + " "*block + "\x1b[0m" + "\x1b[0;36;47m" + " "*(barLength-block) + "\x1b[0m", str(progress*100)[:4], status)
    sys.stdout.write(text)
    sys.stdout.flush()