# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import importlib
import importlib.abc

def load_class_obj(module_path, class_name):
    module = importlib.import_module(module_path)
    class_object = getattr(module, class_name)
    return class_object