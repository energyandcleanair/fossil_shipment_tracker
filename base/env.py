# Getting environmental variables in various ways
# depending on the infrastructure (e.g. GCE, GAE, local etc.)
import os
from decouple import config
from google.cloud.secretmanager_v1 import SecretManagerServiceClient


project_id = config("PROJECT_ID")

cred = config("GOOGLE_APPLICATION_CREDENTIALS", None)
if cred is not None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred

try:
    client = SecretManagerServiceClient()
except Exception:
    client = None


def get_env(key, default=None):
    """
    # This function gets environment either locally or from Google Secrets
    # It allows us to access environmental variables from GAE, or locally
    :param key:
    :param default:
    :return:
    """

    # Try without default
    c = config(key, default=None)
    g = None
    if client and c is None:
        try:
            from base.logger import logger

            logger.info("...Looking for %s in Google Secret" % (key,))
            # Build the resource name of the parent secret.
            parent = client.secret_path(project_id, key)
            versions = client.list_secret_versions(parent=parent)
            names = [x.name for x in versions if (x.State.Name(x.state) == "ENABLED")]

            if len(names) == 1:
                response = client.access_secret_version(name=names[0])
                g = response.payload.data.decode("UTF-8")
                logger.info("Found key: %s" % (key,))

        except Exception as e:
            logger.info("Failed: %s" % (str(e),), exc_info=True, stack_info=True)
            pass

    if c or g:
        return c or g

    else:
        return default
