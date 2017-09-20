# HDF5toCT
Save data from HDF5 files to CloudTurbine

Our goal was to support the conversion of a limited subset of HDF5 files to CT; namely, those with the following structure:

1. datasets must reside under the top parent group in the file

2. each dataset must be of a “compound” datatype which contains 2 channels named “time” and either “data” or “value”

3. the dataspace of each dataset must be a 1-D array of entries (i.e., data must be stored in a 1-D array of compound elements where each element contains 2 channels – “time” and either “data” or “value”)

HDF5toCT documentation can be found at http://www.cloudturbine.com/hdf5-to-ct/
