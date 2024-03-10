from typing import List, Optional

import datetime as dt

from airflow.providers.docker.operators.docker import DockerOperator
from python_docker_operator.interface import ConnectionInterface, ContextInterface


# TODO: WIP, write a custom operator with:
# - custom docker image
# - passing of the correct environment variables
class PythonDockerOperator(DockerOperator):
    """ Custom Docker Operator for easier usage with airflow

    This operator is a wrapper around the DockerOperator from the airflow provider package,
    and handles the following tasks:

    - Docker Image:
        Every project consists of a separate docker image, with the correct python
        dependencies installed, either with pip or conda.
    - Script Path:
        This, is the path used inside the docker container to init execution of python
        code, with the provided arguments.
    - Script Arguments:
        If needed, the operator can pass additional arguments to the python script.
    - Connections:
        Contains read/write of airflow connections, passes them as environment variables
        to the docker container.
    - Context Variables:
        Contains context variables, like execution date, task id, dag id, etc. Passes
        them as environment variables to the docker container.

    """
    ui_color: str = '#1D63ED'
    ui_fgcolor: str = '#E5F2FC'

    def __init__(
        self,
        auto_remove: str = 'success',
        tty: bool = True,
        network_mode: str = 'host',
        custom_file_path: Optional[str] = None,
        custom_cmd_args: Optional[List[str]] = None,
        custom_connection_ids: Optional[List[str]] = None,
        *args, **kwargs
    ):

        # Apply changed default to kwargs
        kwargs['auto_remove'] = auto_remove
        kwargs['tty'] = tty
        kwargs['network_mode'] = network_mode

        ### Handling for custom class attributes
        # Prepare command
        if custom_cmd_args:
            kwargs['command'] = list(filter(None,['python', custom_file_path, *custom_cmd_args]))
        else:
            kwargs['command'] = ['python', custom_file_path]
        
        # Build environment variables, according to provided connection_ids
        # Can later be readed by the python script, when airflow_custom_docker
        if custom_connection_ids:
            for connection_id in custom_connection_ids:
                kwargs['environment'] = {
                    **kwargs.get('environment', {}),
                    **ConnectionInterface(connection_id).dict_all,
                }
        
        # TODO: Add handling for context variables with ContextInterface
        
        super().__init__(*args, **kwargs)

    def execute(self, context):

        # Context is not available during __init__, but can be accessed during execute
        # So, in order to pass context variables as environment variables, we need to
        # override the execute method, and add the context variables to the environment
        self.environment = {
            **self.environment,
            **ContextInterface().dict_all(context),
        }

        super().execute(context)
    


