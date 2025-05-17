"""
Custom exceptions for the Vortex SDK.
"""

class VortexException(Exception):
    """Base exception for all Vortex SDK errors."""
    pass

class VortexConnectionError(VortexException):
    """Raised when there's an issue connecting to the Vortex server."""
    pass

class VortexTimeoutError(VortexException):
    """Raised when an operation times out."""
    pass

class VortexApiError(VortexException):
    """Raised for errors returned by the Vortex API."""
    def __init__(self, message: str, grpc_error: Exception = None, status_code: int = None, details: str = None):
        super().__init__(message)
        self.grpc_error = grpc_error
        self.status_code = status_code
        self.details = details

        if grpc_error:
            if hasattr(grpc_error, 'code') and callable(grpc_error.code):
                try:
                    # grpc.StatusCode is an enum, we might want its name or value
                    grpc_status_code = grpc_error.code()
                    if hasattr(grpc_status_code, 'name'):
                        self.status_code = grpc_status_code.name 
                    elif hasattr(grpc_status_code, 'value') and isinstance(grpc_status_code.value, tuple):
                         self.status_code = grpc_status_code.value[0] # value can be (int, str)
                    else:
                        self.status_code = str(grpc_status_code)
                except Exception:
                    pass # Keep original status_code if any
            
            if hasattr(grpc_error, 'details') and callable(grpc_error.details):
                try:
                    self.details = grpc_error.details()
                except Exception:
                    pass # Keep original details if any
            elif not self.details: # If no details method, use string representation of error
                self.details = str(grpc_error)


    def __str__(self):
        base_str = super().__str__()
        if self.status_code:
            base_str += f" (Status Code: {self.status_code})"
        if self.details:
            base_str += f" Details: {self.details}"
        return base_str

class VortexClientConfigurationError(VortexException):
    """Raised for client configuration errors."""
    pass
