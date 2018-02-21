""" Cache module that implements the SSM caching wrapper """
from __future__ import print_function

from datetime import datetime, timedelta
from functools import wraps
import six

class InvalidParam(Exception):
    """ Raised when something's wrong with the provided param name """

class Refreshable(object):
    def __init__(self, max_age):
        self._last_refresh_time = None
        self._max_age = max_age
        self._max_age_delta = timedelta(seconds=max_age or 0)
    
    def _refresh(self):
        raise NotImplementedError

    def _should_refresh(self):
        # never force refresh if no max_age is configured
        if not self._max_age:
            return False
        # always force refresh if values were never fetched
        if not self._last_refresh_time:
            return True
        # force refresh only if max_age seconds have expired
        return datetime.utcnow() > self._last_refresh_time + self._max_age_delta
    
    def refresh(self):
        self._refresh()
        # keep track of update date for max_age checks
        self._last_refresh_time = datetime.utcnow()

class SSMParameterGroup(Refreshable):
    def __init__(self, max_age=None):
        super(SSMParameterGroup, self).__init__(max_age)
        
        self._parameters = []
    
    def parameter(self, *args, **kwargs):
        parameter = SSMParameter(*args, **kwargs)
        parameter._group = self
        self._parameters.append(parameter)
        return parameter
    
    def _refresh(self):
        for param in self._parameters:
            param._refresh()

class SSMParameter(Refreshable):
    """ The class wraps an SSM Parameter and adds optional caching """
    
    SSM_CLIENT_FACTORY = None
    _SSM_CLIENT = None
    
    @classmethod
    def get_ssm_client(cls, refresh=False):
        if cls._SSM_CLIENT and not refresh:
            return cls._SSM_CLIENT
        if cls.SSM_CLIENT_FACTORY:
            cls._SSM_CLIENT = cls.SSM_CLIENT_FACTORY()
        else:
            import boto3
            cls._SSM_CLIENT = boto3.client('ssm')

    def __init__(self, param_name, max_age=None, with_decryption=True):
        super(SSMParameter, self).__init__(max_age)
        self._name = param_name
        self._value = None
        self._with_decryption = with_decryption
        self._group = None

    def _refresh(self):
        """ Force refresh of the configured param names """
        if self._group:
            return self._group.refresh()
        
        response = self.get_ssm_client().get_parameters(
            Names=[self._name],
            WithDecryption=self._with_decryption,
        )
        # create a dict of name:value for each param
        self._value = response['Parameters']['Value']
        
    @property
    def name(self):
        return self.name

    @property
    def value(self):
        """
            Retrieve the value of a given param name.
            If only one name is configured, the name can be omitted.
        """
        
        if self._value is None or self._should_refresh():
            self.refresh()
        return self._value

    def refresh_on_error(
            self,
            error_class=Exception,
            error_callback=None,
            retry_argument='is_retry'
        ):
        """ Decorator to handle errors and retries """
        def true_decorator(func):
            """ Actual func wrapper """
            @wraps(func)
            def wrapped(*args, **kwargs):
                """ Actual error/retry handling """
                try:
                    return func(*args, **kwargs)
                except error_class:
                    self.refresh()
                    if callable(error_callback):
                        error_callback()
                    kwargs[retry_argument] = True
                    return func(*args, **kwargs)
            return wrapped
        return true_decorator
