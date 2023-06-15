class ClassProperty (property):
    """Subclass property to make classmethod properties possible"""

    def __get__(self, _, owner):
        return self.fget.__get__(None, owner)()  # type:ignore
