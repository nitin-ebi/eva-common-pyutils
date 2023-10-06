import os

import yaml


class Configuration:
    """
    Configuration class that allow to load a yaml file either at construction or later in the execution.
    It can be used like a dict but should be used as readonly.
    """
    config_file = None
    content = {}

    def __init__(self, *search_path):
        if search_path:
            self.load_config_file(*search_path)

    @staticmethod
    def _find_config_file(search_path):
        for p in search_path:
            if p and os.path.isfile(p):
                return p

    def load_config_file(self, *search_path):
        self.config_file = self._find_config_file(search_path)
        if self.config_file:
            with open(self.config_file, 'r') as f:
                self.content = yaml.safe_load(f)
        else:
            raise FileNotFoundError('Could not find any config file in specified search path')

    def get(self, item, ret_default=None):
        """
        Dict-style item retrieval with default
        :param item: The key to search for
        :param ret_default: What to return if the key is not present
        """
        try:
            return self[item]
        except KeyError:
            return ret_default

    def query(self, *parts, ret_default=None):
        """
        Drill down into a config, e.g. cfg.query('logging', 'handlers', 'a_handler', 'level')
        :param ret_default:
        :return: The relevant item if it exists in the config, else ret_default.
        """
        top_level = self.content
        item = None

        for p in parts:
            item = top_level.get(p)
            if item:
                top_level = item
            else:
                return ret_default
        return item

    def report(self):
        return yaml.safe_dump(self.content, default_flow_style=False)

    def __getitem__(self, item):
        """Allow dict-style access, e.g. config['this'] or config['this']['that']."""
        return self.content[item]

    def __contains__(self, item):
        """Allow search in the first layer of the config with 'in' operator."""
        return self.content.__contains__(item)


cfg = Configuration()
"""
Provides a singleton that can be used as a central place for configuration.
"""




class WritableConfig(Configuration):
    """Configuration object that allows writes to the config file"""

    def __init__(self, *search_path, version=None):
        super().__init__(*search_path)
        self.version = version

    def load_config_file(self, *search_path):
        try:
            super().load_config_file(*search_path)
        except FileNotFoundError:
            # expected if it's the first time we are creating the config file
            # In that case the first search path is set to be the config files
            self.config_file = search_path[0]
            pass

    def backup(self):
        """
        Rename the config file by adding a '.1' at the end. If the '.1' file exists it move it to a '.2' and so on.
        """
        if os.path.isfile(self.config_file):
            file_name = self.config_file
            suffix = 1
            backup_name = f'{file_name}.{suffix}'
            while os.path.exists(backup_name):
                suffix += 1
                backup_name = f'{file_name}.{suffix}'

            for i in range(suffix, 1, -1):
                os.rename(f'{file_name}.{i - 1}', f'{file_name}.{i}')
            os.rename(file_name, file_name + '.1')

    def write(self):
        if self.config_file and self.content and os.path.isdir(os.path.dirname(self.config_file)):
            with open(self.config_file, 'w') as open_config:
                yaml.safe_dump(self.content, open_config)

    def set(self, *path, value):
        self._set_version()
        top_level = self.content
        for p in path[:-1]:
            if p not in top_level:
                top_level[p] = {}
            top_level = top_level[p]
        top_level[path[-1]] = value

    def pop(self, *path, default=None):
        """Recursive dictionary pop with default"""
        top_level = self.content
        for p in path[:-1]:
            if p not in top_level:
                return default
            top_level = top_level[p]
        return top_level.pop(path[-1], default)

    def is_empty(self):
        return not self.content

    def clear(self):
        self.content = {}

    def _set_version(self):
        # If we're starting to fill in an empty config, set the version if available
        if self.is_empty() and self.version:
            self.content['version'] = self.version

    def __contains__(self, item):
        return item in self.content

    def __setitem__(self, item, value):
        """Allow dict-style write access, e.g. config['this']='that'."""
        self._set_version()
        self.content[item] = value