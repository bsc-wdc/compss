class Parameter:
    formal_instance = None  # The crate entry for the formal parameter (FormalParameter)
    actual_instance = None  # The crate entry for the actual parameter (PropertyValue)

    def __init__(self, name: str, method: str, direction: str, dtype: [str], isArray: str = "False",
                 description: str = "", value=None):
        self.name = name
        self.method = method
        self.value = value
        self.dtype = dtype
        self.isArray = isArray
        self.direction = direction
        self.description = description

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"Parameter(name='{self.name}', method='{self.method}', direction='{self.direction}', "
            f"type='{self.dtype}', isArray={self.isArray}, description={self.description}, value={repr(self.value)})"
        )
