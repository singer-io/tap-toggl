
#
# Module dependencies.
#

from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import requests
import logging

logger = logging.getLogger()


""" Simple wrapper for Toggl. """
class Toggl(object):

  def __init__(self, api_token=None, start_date=None, user_agent=None, trailing_days=1):
    self.api_token = api_token
    self.trailing_days = int(trailing_days)
    self.workspace_ids = []
    self.user_agent = user_agent


  def _get_workspace_endpoints(self, endpoint):
    endpoints = []
    for workspace_id in self.workspace_ids:
      endpoints.append(endpoint.format(workspace_id=workspace_id))
    return endpoints


  def _get(self, url, **kwargs):
    logger.info("Hitting {url}".format(url=url))
    response = requests.get(url, auth=HTTPBasicAuth(self.api_token, 'api_token'))
    response.raise_for_status()
    return response.json()


  def _get_response(self, url, column_name=None, bookmark=None, key=None):
    res = self._get(url)
    res = [] if res is None else res
    res = res[key] if key is not None else res
    length = len(res)
    logger.info('Endpoint returned {length} rows.'.format(length=length))
    for item in res:
      yield item


  def _get_from_endpoints(self, endpoints, column_name=None, bookmark=None, key=None):
    for endpoint in endpoints:
      gtr = self._get_response(endpoint, key=key)
      for item in gtr:
        yield item


  def is_authorized(self):
    return self._get('https://www.toggl.com/api/v8/me')


  def a_workspaces(self, column_name=None, bookmark=None):
    res = self._get('https://www.toggl.com/api/v8/workspaces')
    self.workspace_ids = []
    for item in res:
      self.workspace_ids.append(item['id'])
      yield item


  def clients(self, column_name=None, bookmark=None):
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/clients')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def groups(self, column_name=None, bookmark=None):
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/groups')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def projects(self, column_name=None, bookmark=None): 
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/projects')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def tasks(self, column_name=None, bookmark=None):
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/tasks')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def tags(self, column_name=None, bookmark=None):
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/tags')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def users(self, column_name=None, bookmark=None):
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/users')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def workspace_users(self, column_name=None, bookmark=None):
    endpoints = self._get_workspace_endpoints('https://www.toggl.com/api/v8/workspaces/{workspace_id}/workspace_users')
    return self._get_from_endpoints(endpoints, column_name, bookmark)


  def time_entries(self, column_name=None, bookmark=None):
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=self.trailing_days)).strftime('%Y-%m-%d')
    endpoints = self._get_workspace_endpoints('https://toggl.com/reports/api/v2/details?workspace_id={{workspace_id}}&since={start_date}&until={end_date}&user_agent={user_agent}'.format(start_date=start_date, end_date=end_date, user_agent=self.user_agent))
    return self._get_from_endpoints(endpoints, column_name, bookmark, "data")    




