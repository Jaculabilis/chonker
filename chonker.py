# Standard library imports
from argparse import ArgumentParser, FileType, ArgumentTypeError
import logging
from signal import signal, SIGPIPE, SIG_DFL
import subprocess
import sys

# Third party imports
from pexpect import EOF
from pexpect.popen_spawn import PopenSpawn as spawn


# Configure SIGPIPE signal handler
signal(SIGPIPE,SIG_DFL)


def filesize(s):
    try:
        if s[-1].upper() == "K":
            return int(s[:-1]) * 1024
        elif s[-1].upper() == "M":
            return int(s[:-1]) * 1024**2
        elif s[-1].upper() == "G":
            return int(s[:-1]) * 1024**3
        return int(s)
    except:
        raise ArgumentTypeError(f"'{s}' is not a valid file size")


def parse_args():
    """
    Helper function for argument parsing.
    """
    parser = ArgumentParser(description="Pass chunks of a stream to another "
        "command invocation")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose "
        "logging.")
    parser.add_argument("--chunk", type=filesize, default=1024, help="Chunk size in bytes.")
    parser.add_argument("--bufsize", type=filesize, default=1024, help="Buffer "
        "size for reading from stdin.")
    parser.add_argument("--exec", required=True, help="Command to pipe chunks to.")
    # parser.add_argument("--inc", help="Pattern to find/replace with the "
    #     "chunk number.")
    # parser.add_argument("--inc-width", type=int, default=3, help="Number of "
    #     "digits to pad the increment with.")
    parser.add_argument("--skip", type=int, default=0, help="Skip a number of "
        "chunks at the start of the input stream.")

    args = parser.parse_args()
    return args


def initialize_logging(verbose):
    """
    Sets up logging.
    """
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    if not verbose:
        logger.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("{message}", style="{")
    else:
        logger.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("[{asctime} {levelname}] {funcName}: {message}", style="{")
    handler.setFormatter(formatter)
    return logger


def chunk_buffers(stream, chunk_size, buf_size):
    """
    A generator that yields buffers from the given stream of size
    buf_size, totaling chunk_size bytes.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"ENTER ({stream.name}, {chunk_size}, {buf_size})")
    total_bytes_read = 0
    while total_bytes_read < chunk_size:
        # Read up to chunk_size bytes
        bytes_to_read = min(chunk_size - total_bytes_read, buf_size)
        logger.debug(f"Read {total_bytes_read} of {chunk_size} bytes, reading {bytes_to_read} bytes")
        buf = stream.read(bytes_to_read)

        # Terminate if no more bytes are available
        if not buf:
            logger.debug(f"EXIT Terminated after {total_bytes_read} of {chunk_size} bytes")
            return
        # Otherwise, update how many bytes have been read and yield them
        bytes_read = len(buf)
        total_bytes_read += bytes_read
        yield buf
    logger.debug(f"EXIT Completed after {total_bytes_read} of {chunk_size} bytes")
    return


def chunk_and_first_buf(buf, chunk):
    """
    Generator wrapper to allow peeking without losing the first buffer.
    """
    yield buf
    for chunk_buf in chunk:
        yield chunk_buf
    return


def next_chunk(stream, chunk_size, buf_size):
    """
    Returns a chunk_buffers generator if there are buffers left to be
    read in the input stream, otherwise returns None.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"ENTER ({stream.name}, {chunk_size}, {buf_size})")
    try:
        chunk = chunk_buffers(stream, chunk_size, buf_size)
        buf = next(chunk)
        if buf:
            logger.debug("EXIT Returning nonempty chunk")
            return chunk_and_first_buf(buf, chunk)
    except:
        # Control reaches here when next(chunk) throws StopIteration
        logger.debug("EXIT No next chunk")
        raise


def input_chunks(stream, chunk_size, buf_size):
    """
    Provides a generator for each chunk in the given stream.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"ENTER ({stream.name}, {chunk_size}, {buf_size})")
    chunk = next_chunk(stream, chunk_size, buf_size)
    while chunk:
        yield chunk
        chunk = next_chunk(stream, chunk_size, buf_size)
    yield chunk
    logger.debug("EXIT No more chunks in input stream")
    return


def pipe_chunk_to_command(command, chunk):
    """
    Executes the given command, passing the chunk to its stdin via buf_size buffers.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Piping chunk | {command}")
    child = spawn(command)
    child.logfile_read = sys.stdout.buffer
    for buf in chunk:
        child.send(buf)
    child.sendeof()
    child.expect(EOF)
    retcode = child.wait()
    logger.debug(f"Process returned {retcode}")
    return retcode


def main():
    """
    Entry function.
    """
    # Parse command line arguments
    args = parse_args()

    logger = initialize_logging(args.verbose)
    logger.debug(args)

    # Skip chunks to get to the beginning of the operating section
    logger.debug(f"Skipping {args.skip} chunks")
    for i in range(args.skip):
        for buf in chunk_buffers(sys.stdin.buffer, args.chunk, args.bufsize):
            pass

    # Process each chunk in the section of the stream to operate on
    logger.debug("Reading chunks")
    for chunk in input_chunks(sys.stdin.buffer, args.chunk, args.bufsize):
        pipe_chunk_to_command(args.exec, chunk)
