
#
# Module dependencies.
#

from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from singer import utils
import backoff
import requests
import logging
import sys


logger = logging.getLogger()


""" Simple wrapper for Toggl. """
class Toggl(object):

  def __init__(self, api_token=None, start_date=None, user_agent=None, trailing_days=1):
    self.api_token = api_token
    self.trailing_days = int(trailing_days)
    self.start_date = start_date
    self.workspace_ids = []
    self.user_agent = user_agent


  def request_too_large(error):
    return error.response.status_code == 503


  def _get_workspace_endpoints(self, endpoint):
    endpoints = []
    for workspace_id in self.workspace_ids:
      endpoints.append(endpoint.format(workspace_id=workspace_id))
    return endpoints


  def _paginate_endpoint(self, endpoint, page=0):
    if "&page=" not in endpoint:
      endpoint += "&page=0"
    else:
        array = endpoint.split('&')
        index = 0
        while index < len(array):
            if "page=" in array[index]:
                array[index] = "page=" + str(page)
            index += 1
        endpoint = '&'.join(array)
    return endpoint


  @backoff.on_exception(backoff.expo,
                        requests.exceptions.RequestException,
                        giveup=request_too_large)
  def _get(self, url, **kwargs):
    logger.info("Hitting {url}".format(url=url))
    response = requests.get(url, auth=HTTPBasicAuth(self.api_token, 'api_token'))
    response.raise_for_status()
    return response.json()


  def _get_response(self, url, column_name=None, bookmark=None, key=None):
    # Special paginated case for `time_entries`, which requires `key` attribute.
    if key == "data":
      page = 0
      length = 1
      while length > 0:
        url = self._paginate_endpoint(url, page)
        res = self._get(url)
        res = res["data"]
        length = len(res)
        logger.info('Endpoint returned {length} rows.'.format(length=length))
        for item in res:
          yield item
        page += 1

    else:
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
    fmt = '%Y-%m-%d'
    end_date = datetime.today().strftime(fmt)
    start_date = (datetime.today() - timedelta(days=self.trailing_days)).strftime(fmt)

    try:
      start_date = utils.strptime_with_tz(bookmark).strftime(fmt)
    except (AttributeError, OverflowError, ValueError, TypeError):
      if bookmark is None:
        start_date = utils.strptime_with_tz(self.start_date).strftime(fmt)
        one_year_ago = (datetime.today() - timedelta(days=364)).strftime(fmt)
        start_date = start_date if start_date > one_year_ago else one_year_ago

    endpoints = []
    moving_start_date = utils.strptime_with_tz(start_date)
    moving_end_date = moving_start_date + timedelta(days=30)
    while moving_end_date < utils.strptime_with_tz(end_date):
      new_endpoints = self._get_workspace_endpoints('https://toggl.com/reports/api/v2/details?workspace_id={{workspace_id}}&since={start_date}&until={end_date}&user_agent={user_agent}'.format(start_date=moving_start_date.strftime(fmt), end_date=moving_end_date.strftime(fmt), user_agent=self.user_agent))
      endpoints.extend(new_endpoints)
      moving_start_date += timedelta(days=30)
      moving_end_date = moving_start_date + timedelta(days=30)

    return self._get_from_endpoints(endpoints, column_name, bookmark, "data")    




