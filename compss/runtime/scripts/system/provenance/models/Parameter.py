class Parameter:
    formal_instance = None      # The crate entry for the formal parameter (FormalParameter)
    actual_instance = None      # The crate entry for the actual parameter (PropertyValue)

    def __init__(self, name: str, method: str, direction: str, dtype: str, value=None):
        self.name = name
        self.method = method
        self.value = value
        self.dtype = dtype
        self.direction = direction