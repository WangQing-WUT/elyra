

# wfeeditor

## Installation

### [Install Miniconda](https://docs.conda.io/en/latest/miniconda.html)
* Get the `build.sh`
```shell
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
```
* Execute the installation script
```shell
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
```
* Verification  
```shell
# need restart shell
conda -h
```
### Setting up wfeeditor environment
* Create a new Python environment using a version that is supported by wfeeditor.
```shell
conda create -n <env-name> python
```
* Activate the new environment
```shell
conda activate <env-name>
```
* Verify your miniconda environment
```shell
python --version # should yield a version that is supported by wfeeditor
which python     # displays current `python` path
pip3 --version   # should be a recent version to avoid build issues
which pip3       # displays current `pip` path
```
* Install a version of Node.js that is supported by wfeeditor.
```shell
conda install -y -c conda-forge/label/main nodejs
```
* Verify node is installed correctly
```shell
node --version 
```
* Install Yarn
```shell
conda install -y -c conda-forge/label/main yarn
```
* Verify yarn is installed correctly
```shell
yarn --version 
```

### Build & Installation


You can build and install all wfeeditor packages with:
```shell
make clean install
```
You can check that the notebook server extension was successfully installed with:
```shell
jupyter serverextension list
```
### Starting wfeeditor
Copy the runtime configuration file to the jupyter directory:
```shell
cp elyra/runtime_config/* ~/.local/share/jupyter/metadata/runtime
```
After verifying wfeeditor has been installed, start wfeeditor with:
 ```bash
jupyter lab --allow-root
```
You can use the wfeeditor according to the methods provided in the startup log below. You can use the browser to access port 8888 of the server to use the wfeeditor.For the first use, you need to enter a token, which can be obtained in the startup log.
```shell
    To access the server, open this file in a browser:
        file:///root/.local/share/jupyter/runtime/jpserver-725259-open.html
    Or copy and paste one of these URLs:
        http://localhost:8888/lab?token=f4315773ab321815ffe9043cd2077e12e14239d01d82f2e6
     or http://127.0.0.1:8888/lab?token=f4315773ab321815ffe9043cd2077e12e14239d01d82f2e6
```
## Container image
### Make images
```shell
make install
make elyra-image
```
### Run images
```shell
chmod 777 [runtime_config_path] [runtime_config.json]
docker run -d -v [runtime_config_path]:/home/jovyan/.local/share/jupyter/metadata/runtimes -p 8888:8888 elyra/elyra:3.12.0
