import asyncio
import logging

import aiohttp

from botocore import endpoint


logger = logging.getLogger(__name__)


@asyncio.coroutine
def convert_to_response_dict(http_response, operation_model):
    """Convert an HTTP response object to a request dict.

    This converts the requests library's HTTP response object to
    a dictionary.

    :type http_response: botocore.vendored.requests.model.Response
    :param http_response: The HTTP response from an AWS service request.

    :rtype: dict
    :return: A response dictionary which will contain the following keys:
        * headers (dict)
        * status_code (int)
        * body (string or file-like object)

    """
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status,
    }
    if response_dict['status_code'] >= 300:
        response_dict['body'] = yield from http_response.read()
    elif operation_model.has_streaming_output:
        raise ValueError("Streaming output")
        #response_dict['body'] = StreamingBody(
        #    http_response.raw, response_dict['headers'].get('content-length'))
    else:
        response_dict['body'] = yield from http_response.read()
    return response_dict


class EndpointCreator(endpoint.EndpointCreator):

    def _get_endpoint_class(self):
        return Endpoint


class Endpoint(endpoint.Endpoint):

    @asyncio.coroutine
    def _send_request(self, request_dict, operation_model):
        attempts = 1
        request = self.create_request(request_dict, operation_model)
        success_response, exception = yield from self._get_response(
            request, operation_model, attempts)
        while self._needs_retry(attempts, operation_model,
                                success_response, exception):
            attempts += 1
            # If there is a stream associated with the request, we need
            # to reset it before attempting to send the request again.
            # This will ensure that we resend the entire contents of the
            # body.
            request.reset_stream()
            # Create a new request when retried (including a new signature).
            request = self.create_request(
                request_dict, operation_model=operation_model)
            success_response, exception = yield from self._get_response(
                request, operation_model, attempts)
        if exception is not None:
            raise exception
        else:
            return success_response

    @asyncio.coroutine
    def _get_response(self, request, operation_model, attempts):
        # This will return a tuple of (success_response, exception)
        # and success_response is itself a tuple of
        # (http_response, parsed_dict).
        # If an exception occurs then the success_response is None.
        # If no exception occurs then exception is None.
        try:
            logger.debug("Sending http request: %s", request)

            http_response = yield from aiohttp.request(
                request.method,
                request.url,
                headers=dict((k, v.decode("utf-8") if hasattr(v, "decode") else v) for (k, v) in request.headers.items()),
                data=request.body,
            )
            #request, #verify=self.verify,
            #stream=operation_model.has_streaming_output,
            #    proxies=self.proxies, timeout=self.timeout)
        except ConnectionError as e:
            # For a connection error, if it looks like it's a DNS
            # lookup issue, 99% of the time this is due to a misconfigured
            # region/endpoint so we'll raise a more specific error message
            # to help users.
            if self._looks_like_dns_error(e):
                endpoint_url = e.request.url
                better_exception = EndpointConnectionError(
                    endpoint_url=endpoint_url, error=e)
                return (None, better_exception)
            else:
                return (None, e)
        except Exception as e:
            logger.debug("Exception received when sending HTTP request.",
                         exc_info=True)
            return (None, e)
        # This returns the http_response and the parsed_data.
        response_dict = yield from convert_to_response_dict(http_response,
                                                 operation_model)
        parser = self._response_parser_factory.create_parser(
            operation_model.metadata['protocol'])
        http_response.status_code = http_response.status
        return ((http_response, parser.parse(response_dict,
                                             operation_model.output_shape)),
                None)
