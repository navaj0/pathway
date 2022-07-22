import unittest
from unittest.mock import patch
from pathway.runtime.bootstrap import bootstrap


class BootstrapTestCase(unittest.TestCase):
    @patch('pathway.runtime.bootstrap._download_env_definition')
    @patch('pathway.runtime.bootstrap._conda_update')
    @patch('pathway.runtime.bootstrap._pip_install')
    def test_something(self, mock_pip_install, mock_conda_update, mock_download):
        bootstrap('s3://my-bucket/requirements.txt')
        mock_pip_install.assert_called_with('requirements.txt')

    @patch('pathway.runtime.bootstrap._download_env_definition')
    @patch('pathway.runtime.bootstrap._conda_update')
    @patch('pathway.runtime.bootstrap._pip_install')
    def test_conda(self, mock_pip_install, mock_conda_update, mock_download):
        bootstrap('s3://my-bucket/env.yml')
        mock_conda_update.assert_called_with('env.yml')


if __name__ == '__main__':
    unittest.main()
