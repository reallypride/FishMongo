from .dispatcher import Signal

class_prepared = Signal(providing_args=["class"])

pre_init = Signal(providing_args=["instance", "args", "kwargs"])
post_init = Signal(providing_args=["instance"])

pre_save = Signal(providing_args=["instance", "using"])
post_save = Signal(providing_args=["instance", "created", "using"])

pre_delete = Signal(providing_args=["instance", "using"])
post_delete = Signal(providing_args=["instance", "using"])

find_spec = Signal(providing_args=["spec"])
