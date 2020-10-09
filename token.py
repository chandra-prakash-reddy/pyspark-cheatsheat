import Crypto.PublicKey.RSA as RSA
import Crypto.Hash.SHA256 as SHA
import Crypto.Signature.PKCS1_v1_5 as PKCS1_v1_5
import base64
import json
import time
from urllib.request import urlopen


try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode


# BEGIN CONFIGURATION - change as needed.

# Path to the JSON file containing the service account private key and email.
PRIVATE_KEY_JSON = 'service-account.json'

# The API scope this token will be valid for.
API_SCOPE = 'https://www.googleapis.com/auth/spreadsheets.readonly'
# The validity of the token in seconds. Max allowed is 3600s.

ACCESS_TOKEN_VALIDITY_SECS = 3600

# END CONFIGURATION


class OauthAccessTokenGetter:
    """Fetches a new Google OAuth 2.0 access token.

    The code is based on the steps described here: https://developers.go
    ogle.com/identity/protocols/OAuth2ServiceAccount#authorizingrequests

    """

    ACCESS_TOKEN_AUD = 'https://www.googleapis.com/oauth2/v4/token'
    REQUEST_URL = 'https://www.googleapis.com/oauth2/v4/token'
    GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:jwt-bearer'

    def __init__(self, private_key_json_file, scope, token_valid_secs=3600):
        self.private_key_json = self.LoadPrivateKeyJsonFromFile(
            private_key_json_file)
        self.scope = scope
        self.token_valid_secs = token_valid_secs

    @classmethod
    def Base64UrlEncode(cls, data):
        """Returns the base64url encoded string for the specified data."""
        return base64.urlsafe_b64encode(data)

    @classmethod
    def LoadPrivateKeyJsonFromFile(cls, private_key_json_file):
        """Returns JSON object by parsing the specified private key JSON
        file."""
        with open(private_key_json_file) as private_key_json_file:
            return json.load(private_key_json_file)

    def GetPrivateKey(self):
        """Returns the imported RSA private key from the JSON data."""
        return RSA.importKey(self.private_key_json['private_key'])

    def GetSigner(self):
        """Returns a PKCS1-V1_5 object for signing."""
        return PKCS1_v1_5.new(self.GetPrivateKey())

    @classmethod
    def GetEncodedJwtHeader(cls):
        """Returns the base64url encoded JWT header."""
        return cls.Base64UrlEncode(json.dumps({'alg': 'RS256', 'typ': 'JWT'}).encode('utf-8'))

    def GetEncodedJwtClaimSet(self):
        """Returns the base64url encoded JWT claim set."""
        current_time_secs = int(time.time())
        jwt_claims = {
            'iss': self.private_key_json['client_email'],
            'scope': self.scope,
            'aud': self.ACCESS_TOKEN_AUD,
            'exp': current_time_secs + self.token_valid_secs,
            'iat': current_time_secs
        }
        return self.Base64UrlEncode(json.dumps(jwt_claims).encode('utf-8'))

    def GetJwtSignature(self, message):
        """Returns signature of JWT as per JSON Web Signature (JWS) spec."""
        signed_message = self.GetSigner().sign(SHA.new(message))
        return self.Base64UrlEncode(signed_message)

    def GetSignedJwt(self):
        """Returns signed JWT."""
        header = self.GetEncodedJwtHeader()
        jwt_claim_set = self.GetEncodedJwtClaimSet()
        signature = self.GetJwtSignature(header + b'.' + jwt_claim_set)
        return header + b'.' + jwt_claim_set + b'.' + signature

    def SendRequest(self, body):
        """Returns the response by sending the specified request."""
        return urlopen(self.REQUEST_URL, urlencode(body).encode('utf-8')).read()

    def GetAccessToken(self):
        """Returns the access token."""
        body = {
            'grant_type': self.GRANT_TYPE,
            'assertion': self.GetSignedJwt()
        }
        response = json.loads(self.SendRequest(body))
        return response['access_token']


if __name__ == '__main__':
    print (OauthAccessTokenGetter(PRIVATE_KEY_JSON, API_SCOPE,
                                  ACCESS_TOKEN_VALIDITY_SECS).GetAccessToken())
