import logging
import Queue

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

from lethril.handlers import StdOutHandler, GenericHandlerWorker
from threading import Thread



# Module level logger
log = logging.getLogger("lethril.twitter")


class TwitterListener(StreamListener):
    """
    """
    def __init__(self, consumer_key, consumer_secret, access_token,
                 access_token_secret, track=None, languages=None,
                 handler=None):
        """
        """

        if handler is None:
            handler = StdOutHandler()

        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.track = track
        self.languages = languages
        self.handler = handler

        # OAuth Credentials
        self.auth = OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(access_token, access_token_secret)

        # Set up Queue
        self.queue = Queue.Queue()

        # Set up a background thread that consumes the queue set up above
        self.worker = GenericHandlerWorker(self.queue, self.handler)
        self.worker.start()


        # Initialise the twitter stream
        self.stream = Stream(self.auth, self)

    def on_data(self, data):
        try:
            self.queue.put(data.strip())
        except:
            log.exception("Unable to put data on the queue")
        return True

    def on_limit(self, track):
        log.warn('Lost messages : %s' % str(track))
        return True

    def on_timeout(self):
        log.info('Timed Out.')

    def on_error(self, status):
        log.error("Status Code : %s" % status)
        return True

    def on_disconnect(self, notice):
        """Called when twitter sends a disconnect notice

        Disconnect codes are listed here:
        https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect
        """
        log.warning(notice)
        return True


    def listen(self):
        log.info("Listening")
        self.stream.filter(track=self.track, languages=self.languages)

    def stop(self):

        # Disconnect stream
        log.warn("Disconnecting Stream...")
        self.stream.disconnect()

        # Block until all queued items are processed
        log.warn("Joining Queue...")
        self.queue.join()

        # Flush items currently in memory to disk
        log.warn("Flushing in-memory items to disk...")
        self.handler._flush()

        log.warn("Listener Stopped.")