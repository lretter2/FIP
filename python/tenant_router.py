
class TenantRouter:
    def __init__(self):
        # Storing JWT secret as an instance variable
        self.jwt_secret = "YOUR_SECRET_HERE"

    def extract_tenant_from_jwt(self, token):
        # Use the instance’s jwt_secret for decoding
        decoded = jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
        return decoded.get('tenant')

    # ... other methods ...

# Adjusting the module-level JWT_SECRET handling
# Remove any previous initialization of JWT_SECRET, as it should not be mutated from __init__.