![Cover](pic/CMAP.png)

# mcmap

mcmap is the MATLAB client for Simons CMAP project. It provides access to the Simons CMAP database where various ocean data sets are hosted. These data sets include multi decades of remote sensing observations (satellite), numerical model estimates, and field expeditions.

This package is adopted from [pycmap](https://github.com/simonscmap/pycmap) which is the python client of Simons CMAP ecosystem. 

## Usage
Clone or download the repository. The source code is in the src directory. The CMAP.m file abstracts the Simons CMAP API and provides the user with several methods to query the database and extract subsets of data. In order to make API requests, you need to get an API key from [Simons CMAP website](https://simonscmap.com). Once you got your API key run the following command (in the MATLAB Command Window) to store the API key on your local machine:

`CMAP.set_api_key('your api key');`

## documentation
In the MATLAB Command Window run the following command to see the docs:

`doc CMAP`

