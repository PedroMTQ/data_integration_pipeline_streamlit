class BaseSingleton:
    """Generic Singleton base class to support multiple unique child instances.

    Subclass __init__ methods are automatically guarded: they run only once
    per singleton instance, so there is no need for manual `_initialized` checks.
    """

    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            instance._singleton_initialized = False
            cls._instances[cls] = instance
        return cls._instances[cls]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        original_init = cls.__dict__.get('__init__')
        if original_init is not None:

            def guarded_init(self, *a, _orig=original_init, **kw):
                if self._singleton_initialized:
                    return
                _orig(self, *a, **kw)
                self._singleton_initialized = True

            cls.__init__ = guarded_init
