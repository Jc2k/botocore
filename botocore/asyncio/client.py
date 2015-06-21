import asyncio

from botocore import client
from botocore.exceptions import ClientError

from .endpoint import EndpointCreator


class ClientCreator(client.ClientCreator):

    def _get_base_client_class(self):
        return BaseClient

    def _get_endpoint_creator_class(self):
        return EndpointCreator


class BaseClient(client.BaseClient):

    @asyncio.coroutine
    def _make_api_call(self, operation_name, api_params):
        operation_model = self._service_model.operation_model(operation_name)
        request_dict = self._convert_to_request_dict(
            api_params, operation_model)
        http, parsed_response = yield from self._endpoint.make_request(
            operation_model, request_dict)

        self.meta.events.emit(
            'after-call.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            http_response=http, parsed=parsed_response,
            model=operation_model
        )

        if http.status_code >= 300:
            raise ClientError(parsed_response, operation_name)
        else:
            return parsed_response
