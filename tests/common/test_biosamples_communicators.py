from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock, patch, PropertyMock

from ebi_eva_common_pyutils.biosamples_communicators import HALCommunicator, WebinHALCommunicator, HttpErrorRetry


class TestHALCommunicator(TestCase):

    @staticmethod
    def patch_token(token='token'):
        """Creates a patch for BSDCommunicator token attribute. it returns the token provided"""
        return patch.object(HALCommunicator, 'token', return_value=PropertyMock(return_value=token))

    def setUp(self) -> None:
        self.comm = HALCommunicator('http://aap.example.org', 'http://BSD.example.org', 'user', 'pass')

    def test_token(self):
        with patch('requests.get', return_value=Mock(text='token', status_code=200)) as mocked_get:
            self.assertEqual(self.comm.token, 'token')
            mocked_get.assert_called_once_with('http://aap.example.org', auth=('user', 'pass'))

    def test_req(self):
        with patch('requests.request', return_value=Mock(status_code=200)) as mocked_request, \
                patch.object(HALCommunicator, 'token', new_callable=PropertyMock(return_value='token')):
            self.comm._req('GET', 'http://BSD.example.org')
            mocked_request.assert_called_once_with(
                method='GET', url='http://BSD.example.org',
                headers={'Accept': 'application/hal+json', 'Authorization': 'Bearer token'}
            )

        # 500 should trigger a retry
        with patch.object(HALCommunicator, 'token', new_callable=PropertyMock(return_value='token')), \
                patch('requests.request') as mocked_request:
            mocked_request.return_value = Mock(status_code=500, request=PropertyMock(url='text'))
            self.assertRaises(HttpErrorRetry, self.comm._req, 'GET', 'http://BSD.example.org')

        # 404 should fail with a ValueError, but not trigger a retry
        with patch.object(HALCommunicator, 'token', new_callable=PropertyMock(return_value='token')), \
                patch('requests.request') as mocked_request:
            mocked_request.return_value = Mock(status_code=404, request=PropertyMock(url='text'))
            try:
                self.comm._req('GET', 'http://BSD.example.org')
            except HttpErrorRetry:
                self.fail('Request should not raise HttpErrorRetry')
            except ValueError:
                return

    def test_root(self):
        expected_json = {'json': 'values'}
        with patch.object(HALCommunicator, '_req') as mocked_req:
            mocked_req.return_value = Mock(json=Mock(return_value={'json': 'values'}))
            self.assertEqual(self.comm.root, expected_json)
            mocked_req.assert_called_once_with('GET', 'http://BSD.example.org')

    def test_follows(self):
        json_response = {'json': 'values'}
        # Patches the _req function that returns the Response object with a json function
        patch_req = patch.object(HALCommunicator, '_req', return_value=Mock(json=Mock(return_value=json_response)))

        # test follow url
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows('test', {'test': 'url'}), json_response)
            mocked_req.assert_any_call('GET', 'url')

        # test follow url with a template
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows('test', {'test': 'url/{id:*.}'}, url_template_values={'id': '1'}),
                             json_response)
            mocked_req.assert_any_call('GET', 'url/1')

        # test follow url deep in the json_obj
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows('test1.test2', {'test1': {'test2': 'url'}}),  json_response)
            mocked_req.assert_any_call('GET', 'url')

        # test follow url wih specific verb and payload
        with patch_req as mocked_req:
            self.assertEqual(
                self.comm.follows('test', {'test': 'url'}, method='POST', json={'data': 'value'}),
                json_response
            )
            mocked_req.assert_any_call('POST', 'url', json={'data': 'value'})

        # test follow with depagination
        json_entries_with_next = {
            '_embedded': {'samples': [json_response, json_response]},
            '_links': {'next': {'href': 'url'}, 'first': {}, 'last': {}},
            'page': {}
        }
        json_entries_without_next = {
            '_embedded': {'samples': [json_response]},
            '_links': {},
            'page': {}
        }
        patch_req_with_pages = patch.object(HALCommunicator, '_req', side_effect=[
            Mock(json=Mock(return_value=deepcopy(json_entries_with_next))),
            Mock(json=Mock(return_value=deepcopy(json_entries_with_next))),
            Mock(json=Mock(return_value=deepcopy(json_entries_with_next))),
            Mock(json=Mock(return_value=deepcopy(json_entries_without_next))),
        ])
        # Without all_pages=True only returns the first page
        with patch_req_with_pages as mocked_req:
            observed_json = self.comm.follows('test', {'test': 'url'})
            self.assertEqual(observed_json, json_entries_with_next)
            self.assertEqual(len(observed_json['_embedded']['samples']), 2)
            mocked_req.assert_any_call('GET', 'url')

        # With all_pages=True returns the first page that contains all the embedded elements
        with patch_req_with_pages as mocked_req:
            observed_json = self.comm.follows('test', {'test': 'url'}, all_pages=True)
            self.assertEqual(len(observed_json['_embedded']['samples']), 7)
            self.assertEqual(mocked_req.call_count, 4)

    def test_follows_link(self):
        json_response = {'json': 'values'}
        # Patches the _req function that returns the Response object with a json function
        patch_req = patch.object(HALCommunicator, '_req', return_value=Mock(json=Mock(return_value=json_response)))

        # test basic follow
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows_link('test', {'_links': {'test': {'href': 'url'}}}), json_response)
            mocked_req.assert_any_call('GET', 'url')


class TestWebinHALCommunicator(TestCase):

    def setUp(self) -> None:
        self.comm = WebinHALCommunicator('http://webin.example.org', 'http://BSD.example.org', 'user', 'pass')

    def test_communicator_attributes(self):
        assert self.comm.communicator_attributes == {'webinSubmissionAccountId': 'user'}

    def test_token(self):
        with patch('requests.post', return_value=Mock(text='token', status_code=200)) as mocked_post:
            self.assertEqual(self.comm.token, 'token')
            print(mocked_post.mock_calls)
            mocked_post.assert_called_once_with('http://webin.example.org',
                                                json={'authRealms': ['ENA'], 'password': 'pass', 'username': 'user'})
