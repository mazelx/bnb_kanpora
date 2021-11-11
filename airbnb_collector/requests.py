#!/usr/bin/python3
"""
Functions to request data from the Airbnb web site, and to manage
a set of requests.

Tom Slee, 2013--2017.
"""
import logging
import random
import time
import requests

# Set up logging
LOGGER = logging.getLogger()

class ABRequest():
    def __init__(self, config) -> None:
        self.config = config

    def ws_request_with_repeats(self, url, params=None):
        """ An attempt to get data from Airbnb. The function wraps
        a number of individual attempts, each of which may fail
        occasionally, in an attempt to get a more reliable
        data set.

        Returns None on failure
        """
        LOGGER.debug("URL for this search: %s", url)
        for attempt_id in range(self.config.MAX_CONNECTION_ATTEMPTS):
            try:
                response = self.ws_individual_request(url, attempt_id, params)
                if response is None:
                    continue
                elif response.status_code == requests.codes.ok:
                    return response
            except (SystemExit, KeyboardInterrupt):
                raise
            except AttributeError:
                LOGGER.exception("AttributeError retrieving page")
            except Exception as ex:
                LOGGER.error("Failed to retrieve web page %s", url)
                LOGGER.exception("Exception retrieving page: %s", str(type(ex)))
                # Failed
        return None


    def ws_individual_request(self, url, attempt_id, params=None):
        """
        Individual web request: returns a response object or None on failure
        """
        try:
            # wait
            sleep_time = self.config.REQUEST_SLEEP * random.random()
            LOGGER.debug("sleeping " + str(sleep_time)[:7] + " seconds...")
            time.sleep(sleep_time)  # be nice

            timeout = self.config.HTTP_TIMEOUT

            # If a list of user agent strings is supplied, use it
            if len(self.config.USER_AGENT_LIST) > 0:
                user_agent = random.choice(self.config.USER_AGENT_LIST)
                headers = {"User-Agent": user_agent}
            else:
                headers = {'User-Agent': 'Mozilla/5.0'}

            # If there is a list of proxies supplied, use it
            http_proxy = None
            LOGGER.debug("Using " + str(len(self.config.HTTP_PROXY_LIST)) + " proxies")
            if len(self.config.HTTP_PROXY_LIST) > 0:
                http_proxy = random.choice(self.config.HTTP_PROXY_LIST)
                proxies = {
                    'http': 'http://' + http_proxy,
                    'https': 'https://' + http_proxy,
                }
                LOGGER.debug("Requesting page through proxy %s", http_proxy)
            else:
                proxies = None
                LOGGER.debug("Requesting page without using a proxy")

            # Now make the request
            # cookie to avoid auto-redirect
            cookies = dict(sticky_locale='en')
            response = requests.get(url, params, timeout=timeout,
                                    headers=headers, cookies=cookies, proxies=proxies)
            if response.status_code < 300:
                return response
            else:
                if http_proxy:
                    LOGGER.warning(
                        "HTTP status %s from web site: IP address %s may be blocked",
                        response.status_code, http_proxy)
                    if len(self.config.HTTP_PROXY_LIST) > 0:
                        # randomly remove the proxy from the list, with probability 50%
                        if random.choice([True, False]):
                            self.config.HTTP_PROXY_LIST.remove(http_proxy)
                            LOGGER.warning(
                                "Removing %s from proxy list; %s of %s remain",
                                http_proxy, len(self.config.HTTP_PROXY_LIST),
                                len(self.config.HTTP_PROXY_LIST_COMPLETE))
                        else:
                            LOGGER.warning(
                                "Not removing %s from proxy list this time; still %s of %s",
                                http_proxy, len(self.config.HTTP_PROXY_LIST),
                                len(self.config.HTTP_PROXY_LIST_COMPLETE))
                    if len(self.config.HTTP_PROXY_LIST) == 0:
                        # fill proxy list again, wait a long time, then restart
                        LOGGER.warning(("No proxies remain."
                                        "Resetting proxy list and waiting %s minutes."),
                                    (self.config.RE_INIT_SLEEP_TIME / 60.0))
                        self.config.HTTP_PROXY_LIST = list(self.config.HTTP_PROXY_LIST_COMPLETE)
                        time.sleep(self.config.RE_INIT_SLEEP_TIME)
                        self.config.REQUEST_SLEEP += 1.0
                        LOGGER.warning("Adding one second to request sleep time.  Now %s",
                                    self.config.REQUEST_SLEEP)
                else:
                    LOGGER.warning(("HTTP status %s from web site: IP address blocked. "
                                    "Waiting %s minutes."),
                                response.status_code, (self.config.RE_INIT_SLEEP_TIME / 60.0))
                    time.sleep(self.config.RE_INIT_SLEEP_TIME)
                    self.config.REQUEST_SLEEP += 1.0
                return response
        except (SystemExit, KeyboardInterrupt):
            raise
        except requests.exceptions.ConnectionError:
            # For requests error and exceptions, see
            # http://docs.python-requests.org/en/latest/user/quickstart/
            # errors-and-exceptions
            LOGGER.warning("Network request %s: connectionError. Bad proxy %s ?",
                        attempt_id, http_proxy)
            return None
        except requests.exceptions.HTTPError:
            LOGGER.error(
                "Network request exception %s (invalid HTTP response), for proxy %s",
                attempt_id, http_proxy)
            return None
        except requests.exceptions.Timeout:
            LOGGER.warning(
                "Network request exception %s (timeout), for proxy %s",
                attempt_id, http_proxy)
            return None
        except requests.exceptions.TooManyRedirects:
            LOGGER.error("Network request exception %s: too many redirects", attempt_id)
            return None
        except requests.exceptions.RequestException:
            LOGGER.error("Network request exception %s: unidentified requests", attempt_id)
            return None
        except Exception as e:
            LOGGER.exception("Network request exception: type %s", type(e).__name__)
            return None
