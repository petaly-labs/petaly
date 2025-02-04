import unittest

from petaly.cli.cli import Cli

class TestStringMethods(unittest.TestCase):

    def test_cli_show(self):
        cli = Cli()
        cli.start()
        self.assertEqual('foo'.upper(), 'FOO')


if __name__ == '__main__':
    unittest.main()

