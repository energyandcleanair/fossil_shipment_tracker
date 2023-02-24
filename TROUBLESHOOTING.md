# Mac users
Various commands that turned out useful in setting up environment on a Mac.

```bash
 brew uninstall osgeo-postgresql
 brew uninstall postgresql
 brew install postgresql
 brew install postgis
```

GRPC
```
pip uninstall grpcio
export GRPC_PYTHON_LDFLAGS=" -framework CoreFoundation"
pip install grpcio --no-binary :all:
```

```
brew install geos
DYLD_LIBRARY_PATH=/opt/homebrew/opt/geos/lib/
```

```commandline
pip uninstall grpcio
conda install grpcio
```

## Libraries

### Library not loaded: '@rpath/libssl.3.dylib'

```commandline
brew unlink openssl && brew link openssl --force
sudo ln -s /opt/homebrew/opt/openssl@3/lib/libssl.3.dylib /usr/local/lib/libssl.3.dylib
sudo ln -s /opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib /usr/local/lib/libcrypto.3.dylib
```
