/*
Copyright 2017 Erigo Technologies LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import cycronix.ctlib.*;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5G_info_t;
import hdf.hdf5lib.exceptions.HDF5Exception;
import hdf.hdf5lib.structs.H5O_info_t;
import hdf.object.Attribute;
import hdf.object.Datatype;
import hdf.object.h5.H5File;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Read data from an HDF5 file and send it to CloudTurbine
 *
 * The following code was accessed for ideas on reading COMPOUND data:
 * (a) HDFView code, src\hdf\object\h5\H5CompoundDS.java, function extractCompoundInfo()
 *        helpful that this is Java code
 *        also see function read(): made to handle any flavor of H5 file, so pretty gnarly
 * (b) Java example H5Ex_T_Compound.java found at https://support.hdfgroup.org/ftp/HDF5/examples/examples-by-api/java/examples/datatypes/
 *        looks promising, not too complex; see ReadDataset() around line 350
 * (c) h5dump code; see CloudTurbine/HDF5/hdf5-1.10.1/tools/src/h5dump
 *        C-code, so not sure how useful it will be
 *
 *  Another good example to reference on creating and reading a Dataset is:
 *  C:/Users/johnw/user/CloudTurbine/HDF5/hdf5_examples/HDF5Examples/JAVA/HDF5DatasetRead.java
 *
 *  Program limitations:
 *  --------------------
 *  (a) Program doesn't handle unsigned data; if the HDF5 file contains "unsigned int", this program simply
 *      stores this as an int (similar for "unsigned long" saved to a long and "unsigned short" saved to a short)
 *  (b) HDF5 file is saved in "channel then time" format (similar to how files would be stored in a file system);
 *      for CloudTurbine efficiency, we switch this around and store the data in CT as "time then channel"; to
 *      accomplish this, ALL the data from every channel in the HDF5 file is read and stored in a large Map
 *      (using Google TreeMultimap); this is "brute force" and would run into problems with large datasets;
 *      see more notes on this below (search for "TreeMultimap").
 *  (c) This is not a general purpose HDF5-to-CT translator.  Notes on the expected format of the data files:
 *      we read data from Datasets in the top parent group; this Dataset must use a Compound
 *      Datatype and the data in the Dataset must be a 1-D array of these Compound elements; each Compound element
 *      must contain 2 channels, named "time" and either "data" or "value".
 */
public class HDF5toCT {
    private String inFileFullPathName = null;   // full path to the file
    private String inFileName = null;           // just the name of the file
    private String encryptionPW = null;         // Encryption password; if this remains null, we will not encrypt.
    private double flushInterval = 1.0;         // auto-flush interval in seconds
    private double baseTime = 1483246800.0;     // base time (seconds since epoch) to be added to all times from the HDF5 file; this default is Jan 1, 2017 at 00:00:00 GMT-5
    private boolean bZip = true;                // ZIP data?
    private boolean bGzip = false;              // GZIP data?  We will also ZIP data if this is true.
    private boolean bPack = false;              // pack data?
    private boolean bHiResTime = false;         // use high resolution (microsecond) time for CT data?
    private boolean bAttributesToFile = false;  // Send attributes to a standard file rather than writing them out via CT?

    public static void main(String args[]) throws Exception {
        new HDF5toCT(args);
    }

    public HDF5toCT(String argsI[]) throws Exception {
        long fid = -1;
        long dataset_id = -1;

        //
        // Parse command line arguments
        //
        // 1. Setup command line options
        //
        Options options = new Options();
        // Boolean options (only the flag, no argument)
        options.addOption("h", "help", false, "Print this message.");
        options.addOption("nz", "nozip", false, "Turn off ZIP output.");
        options.addOption("p", "pack", false, "Pack data.");
        options.addOption("hrt", "hirestime", false, "Use high resolution (microsecond) time for CT data.");
        options.addOption("af", "attrtofile", false, "Write attributes to file (not standard CT output).");
        options.addOption("g", "gzip", false, "GZIP output data; data will also be ZIP'ed if this option is selected.");
        // Command line options that include a flag
        Option option = Option.builder("i")
                .longOpt("infile")
                .argName("hdf5file")
                .hasArg()
                .desc("Full path of the input HDF5 file.")
                .build();
        options.addOption(option);
        option = Option.builder("f")
                .argName("autoFlush")
                .hasArg()
                .desc("Flush interval (sec); amount of data per block; default = " + Double.toString(flushInterval))
                .build();
        options.addOption(option);
        option = Option.builder("b")
                .argName("basetime")
                .hasArg()
                .desc("Base time (seconds since epoch) to be added to all timestamps from the HDF5 file; default = " + ""+baseTime)
                .build();
        options.addOption(option);
        option = Option.builder("e")
                .argName("password")
                .hasArg()
                .desc("Encrypt the CT source using the given password.")
                .build();
        options.addOption(option);

        //
        // 2. Parse command line options
        //
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse( options, argsI );
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Command line argument parsing failed: " + exp.getMessage() );
            return;
        }

        //
        // 3. Retrieve the command line values
        //
        if (line.hasOption("h")) {
            // Display help message and quit
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(160);
            formatter.printHelp( "HDF5toCT", "", options, "NOTE: Make sure \"hdf5_java.dll\" is in the same directory as HDF5toCT.jar" );
            System.exit(0);
        }
        bZip = !line.hasOption("nz");
        bGzip = line.hasOption("g");
        if ( !bZip && bGzip ) {
            System.err.println("You cannot turn off ZIP'ing when GZIP has been turned on.");
            System.exit(0);
        }
        bPack = line.hasOption("p");
        bHiResTime = line.hasOption("hrt");
        bAttributesToFile = line.hasOption("af");
        inFileFullPathName = line.getOptionValue("i");
        if ( (inFileFullPathName == null) || (inFileFullPathName.isEmpty()) ) {
            System.err.println("You must specify the name of an HDF5 file using the \"-i\" flag.");
            System.exit(0);
        }
        File inputF = new File(inFileFullPathName);
        if (!inputF.exists()) {
            System.err.println("The given input file, \"" + inFileFullPathName + "\" does not exist.");
            System.exit(0);
        }
        inFileName = inputF.getName();
        flushInterval = Double.parseDouble(line.getOptionValue("f",""+flushInterval));
        if (flushInterval <= 0.0) {
            System.err.println("Flush interval must be greater than 0.0");
            System.exit(0);
        }
        baseTime = Double.parseDouble(line.getOptionValue("b",""+baseTime));
        if (baseTime < 0.0) {
            System.err.println("Base time must be greater than or equal to 0.0");
            System.exit(0);
        }
        encryptionPW = line.getOptionValue("e",null);

        // Load the HDF JNI library
        // We include a copy of this library in the JAR file; problem is, it isn't possible to load a DLL
        // directly from a JAR file.  Possible solution is to use Adam Heinrich's NativeUtils class:
        //     https://www.adamheinrich.com/blog/2012/12/how-to-load-native-jni-library-from-jar/
        //     https://github.com/adamheinrich/native-utils
        //     https://stackoverflow.com/questions/2937406/how-to-bundle-a-native-library-and-a-jni-library-inside-a-jar
        try {
            System.loadLibrary("hdf5_java"); // hdf5_java.dll needs to be on classpath (or it can be in same directory as the JAR file for this program)
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Unable to load hdf5_java.dll; exiting");
            return;
        }

        // Open the HDF5 file
        try {
            fid = H5.H5Fopen(inFileFullPathName, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        String rootGroup = "/";
        long gid = H5.H5Gopen(fid, rootGroup, HDF5Constants.H5P_DEFAULT);

        // Get attributes for the root level object ("/") and write them to CT or file
        CTwriter attributesCTW = null;
        if (bAttributesToFile) {
            // Write attributes to non-standard (non-CT) location
            writeAttributesToFile("CTdata" + File.separator + inFileName + File.separator + inFileName + ".txt", gid, "");
        } else {
            attributesCTW = new CTwriter("CTdata/" + inFileName + rootGroup + "_Attributes");
            attributesCTW.setTime(System.currentTimeMillis() / 1000.0);
            attributesCTW.autoSegment(0); // no segments
            writeAttributesToCT(attributesCTW, gid, inFileName + ".txt", "");
            attributesCTW.close();
        }

        // Only fetch data from one top Group, the "parent" group if you will, located at "/<parent_group>"
        // If there is more than one top parent_group, we just ignore the others
        // Determine the name of this top Group

        H5G_info_t info = H5.H5Gget_info(gid);
        int nelems = (int) info.nlinks;
        if (nelems <= 0) {
            System.err.println("The top parent Group does not contain any child Groups");
            return;
        }
        // Get information on what is in this Group
        // Following is taken from HDFView code, src\hdf\object\h5\H5File.java, see line 2253
        int[] objTypes = new int[nelems];
        long[] fNos = new long[nelems];
        long[] objRefs = new long[nelems];
        String[] objNames = new String[nelems];
        try {
            H5.H5Gget_obj_info_full(fid, rootGroup, objNames, objTypes, null, fNos, objRefs, HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC);
        }
        catch (HDF5Exception ex) {
            System.err.println("Unable to get information from the specified HDF5 file:\n" + ex);
            ex.printStackTrace();
            H5.H5Gclose(gid);  // close the Group
            H5.H5Fclose(fid);  // close the File
            return;
        }
        String topParentGroup = "";
        for (int i=0; i<nelems; ++i) {
            if (objTypes[i] == HDF5Constants.H5O_TYPE_GROUP) {
                topParentGroup = objNames[i];
                System.err.println("\nTop parent Group = " + topParentGroup);
                break;
            }
        }
        if (topParentGroup.isEmpty()) {
            System.err.println("There are no child Groups under \"" + rootGroup + "\"; exiting");
            H5.H5Gclose(gid);  // close the Group
            H5.H5Fclose(fid);  // close the File
            return;
        }

        H5.H5Gclose(gid);  // close the Group; we re-open it below

        // Get information on all the Groups under the top parent Group
        gid = H5.H5Gopen(fid, topParentGroup, HDF5Constants.H5P_DEFAULT);
        info = H5.H5Gget_info(gid);
        nelems = (int) info.nlinks;
        if (nelems <= 0) {
            System.err.println("Top group in the file, \"" + topParentGroup + "\", does not contains any child groups; exiting");
            H5.H5Gclose(gid);  // close the Group
            H5.H5Fclose(fid);  // close the File
            return;
        }
        // Get information on what is in this Group
        // Following is taken from HDFView code, src\hdf\object\h5\H5File.java, see line 2253
        objTypes = new int[nelems];
        fNos = new long[nelems];
        objRefs = new long[nelems];
        objNames = new String[nelems];
        try {
            H5.H5Gget_obj_info_full(fid, topParentGroup, objNames, objTypes, null, fNos, objRefs, HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC);
        }
        catch (HDF5Exception ex) {
            System.err.println("Unable to get information from the specified HDF5 file:\n" + ex);
            ex.printStackTrace();
            H5.H5Gclose(gid);  // close the Group
            H5.H5Fclose(fid);  // close the File
            return;
        }

        //
        // Write attributes for the top parent group to CT or file
        //
        if (bAttributesToFile) {
            // Write attributes to non-standard (non-CT) location
            writeAttributesToFile("CTdata" + File.separator + inFileName + File.separator + topParentGroup + File.separator + topParentGroup + ".txt", gid, "");
        } else {
            // Keep this CTwriter open; we will write the attributes for all the channels within this group with this same CTwriter
            attributesCTW = new CTwriter("CTdata/" + inFileName + rootGroup + topParentGroup + "/_Attributes");
            attributesCTW.setTime(System.currentTimeMillis() / 1000.0);
            attributesCTW.autoSegment(0); // no segments
            writeAttributesToCT(attributesCTW, gid, topParentGroup + ".txt", "");
        }

        //
        // Explore all child Datasets under this Group (ignore non-Dataset objects)
        //

        //
        // The HDF5 files are organized like a file system: file structure branching out to time/value data at
        // the nodes; the file structure comes first then values are organized by time; this could be called
        // this could be called organizing by "channels then time".  For example:
        //
        // foo.h5
        //     Foo1
        //         chan1
        //             time=9326063.9173, value=1.1
        //             time=9326064.0173, value=1.2
        //             etc
        //         chan2
        //             time=9326063.8323, value=1.3
        //             time=9326063.9324, value=1.4
        //             etc
        //         etc
        //
        // For CT efficiency, we want to store data the opposite way: sort by "time then channels". Using the
        // same example as above, this would look like:
        //
        // foo.h5
        //     Foo1
        //         9326063.8323
        //             chan2 (file containing the value 1.3)
        //         9326063.9173
        //             chan1 (file containing the value 1.1)
        //         9326063.9324
        //             chan2 (file containing the value 1.4)
        //         9326064.0173
        //             chan1 (file containing the value 1.2)
        //
        // Problem is, to resort the data by time like this involves scanning over *all* channels.  The easiest (brute-
        // force) way to reorganize the data in this manner is to read *all* of the data from *all* channels into a
        // large key/value map, where each key is a time and each value is a Collection/Array holding one or more
        // objects which contain both the channel name and the data value.  Once all the data is loaded into such
        // a map, we would then sort by key value (ie, sort by time) and then iterate through the map and write
        // to CT.  A couple points about this:
        //
        //     1. note that the value needs to be a Collection/Array of objects; this is because standard Java maps
        //        don't support duplicate keys (ie, can't have 2 identical keys each with their own unique value)
        //     2. since we need to be able to sort the keys, we would use a TreeMap (which implements SortedMap)
        //
        // Here are links on such an implementation:
        //
        //     https://stackoverflow.com/questions/1062960/map-implementation-with-duplicate-keys
        //         ** See the simple implementation from user "user668943" posted on 2011-06-15
        //     https://stackoverflow.com/questions/18922165/how-to-include-duplicate-keys-in-hashmap
        //     https://stackoverflow.com/questions/922528/how-to-sort-map-values-by-key-in-java
        //
        // An alternative to using the standard Java TreeMap as described above is to use Google's TreeMultimap class
        // (from their open source Guava library) which supports duplicate keys and sorted keys and values:
        //
        //     http://google.github.io/guava/releases/snapshot/api/docs/com/google/common/collect/TreeMultimap.html
        //     https://github.com/google/guava/wiki/NewCollectionTypesExplained#multimap
        //     http://tomjefferys.blogspot.com/2011/09/multimaps-google-guava.html
        //         ** good simple example
        //
        // We use TreeMultimap here.  Since TreeMultimap sorts both keys and values, both the key and value objects
        // need to implement Comparable.  We use Double for the keys, which already implements Comparable.  We had
        // to make sure our value class, HDFValue (which is defined below) implements Comparable.
        //

        Multimap<Double, HDFValue> dataMap = TreeMultimap.create();

        //
        // Iterate over all the objects in the top parent Group
        // Filter through all these objects to find the ones we will work with:
        // - must be a Dataset
        // - the Datatype must be Compound
        // - each Compound element must contain 2 entries with names "time" and either "data" or "value"
        // - the Dataspace must be a 1-D array (rank=1)
        //
        for (int i=0; i<nelems; ++i) {
            //
            // We only currently support Datasets; ignore other types of objects
            //
            if (objTypes[i] == HDF5Constants.H5O_TYPE_GROUP) {
                System.err.println(objNames[i] + "\ttype = GROUP; not currently handled");
                continue;
            } else if (objTypes[i] == HDF5Constants.H5O_TYPE_NAMED_DATATYPE) {
                System.err.println(objNames[i] + "\ttype = DATATYPE; not currently handled");
                continue;
            } else if (objTypes[i] == HDF5Constants.H5O_TYPE_NTYPES) {
                System.err.println(objNames[i] + "\ttype = NTYPES; not currently handled");
                continue;
            } else if (objTypes[i] == HDF5Constants.H5O_TYPE_UNKNOWN) {
                System.err.println(objNames[i] + "\ttype = UNKNOWN; not currently handled");
                continue;
            } else if (objTypes[i] != HDF5Constants.H5O_TYPE_DATASET) {
                System.err.println(objNames[i] + "\ttype = " + objTypes[i] + "; since it isn't H5O_TYPE_DATASET, we don't currently handle it");
                continue;
            }

            //
            // Open the Dataset
            //
            String datasetName = rootGroup + topParentGroup + "/" + objNames[i];
            long did = H5.H5Dopen(fid, datasetName, HDF5Constants.H5P_DEFAULT);
            if (did < 0) {
                System.err.println("Could not open dataset " + datasetName);
                continue;
            }

            //
            // Examine the Datatype, which must be Compound type having 2 members;
            // first channel to be "time" and the second to be either "data" or "value"
            //
            long tid = H5.H5Dget_type(did);
            int tclass = H5.H5Tget_class(tid);
            if (tclass != HDF5Constants.H5T_COMPOUND) {
                System.err.println("Dataset " + datasetName + " has Datatype = " + tclass + "; not Compound, ignoring");
                H5.H5Tclose(tid); // close the Datatype
                H5.H5Dclose(did); // close the Dataset
                continue;
            }
            System.err.println("\nDataset " + datasetName);
            long datatype_len = H5.H5Tget_size(tid);  // this will be the number of bytes in one element (ie, one Compound element)
            int num_members = H5.H5Tget_nmembers(tid);
            if (num_members != 2) {
                System.err.println("Dataset " + datasetName + ": Datatype doesn't have 2 members as expected");
                H5.H5Tclose(tid); // close the Datatype
                H5.H5Dclose(did); // close the Dataset
                continue;
            }
            // Store the datatype of each Compound member in an array of DatatypeElementSpecification objects
            // (ie, this should be an array of size = 2)
            DatatypeElementSpecification[] datatypeElements = new DatatypeElementSpecification[num_members];
            boolean bMemberTypeErr = false;
            for (int j = 0; j < num_members; ++j) {
                DatatypeElementSpecification des = new DatatypeElementSpecification();
                String member_name = H5.H5Tget_member_name(tid, j);
                long member_offset = H5.H5Tget_member_offset(tid, j);
                long member_type = H5.H5Tget_member_type(tid, j);
                long member_size = H5.H5Tget_size(member_type);
                int member_class = H5.H5Tget_member_class(tid, j);
                // Either of the following should return a string like "H5T_FLOAT" or "H5T_INTEGER"
                // String member_class_name = H5.H5Tget_class_name(member_class);
                // String member_class_name = H5.H5Tget_class_name(H5.H5Tget_class(member_type));
                String member_class_name = "";
                if ( (member_class == HDF5Constants.H5T_FLOAT) && (member_size == 8) ) {
                    member_class_name = "double";
                } else if ( (member_class == HDF5Constants.H5T_FLOAT) && (member_size == 4) ) {
                    member_class_name = "float";
                } else if ( (member_class == HDF5Constants.H5T_INTEGER) && (member_size == 8) ) {
                    int tsign = H5.H5Tget_sign(member_type);
                    if (tsign == HDF5Constants.H5T_SGN_NONE) {
                        member_class_name = "unsigned long";
                    } else {
                        member_class_name = "long";
                    }
                } else if ( (member_class == HDF5Constants.H5T_INTEGER) && (member_size == 4) ) {
                    int tsign = H5.H5Tget_sign(member_type);
                    if (tsign == HDF5Constants.H5T_SGN_NONE) {
                        member_class_name = "unsigned int";
                    } else {
                        member_class_name = "int";
                    }
                } else if ( (member_class == HDF5Constants.H5T_INTEGER) && (member_size == 2) ) {
                    int tsign = H5.H5Tget_sign(member_type);
                    if (tsign == HDF5Constants.H5T_SGN_NONE) {
                        member_class_name = "unsigned short";
                    } else {
                        member_class_name = "short";
                    }
                } else {
                    bMemberTypeErr = true;
                    break;
                }
                des.name = member_name;
                des.type = member_class_name;
                des.size = member_size;
                des.offset = member_offset;
                datatypeElements[j] = des;
                System.err.println("\t" + des.toString());
                // Native type appear to be the same as the main member type
                // long native_tid = H5.H5Tget_native_type(sub_tid);
                // String native_class_name = H5.H5Tget_class_name(H5.H5Tget_class(native_tid));
                // long native_size = H5.H5Tget_size(native_tid);
            }
            if (bMemberTypeErr) {
                System.err.println("Dataset " + datasetName + ": error with a Compound datatype member of unknown type");
                H5.H5Tclose(tid); // close the Datatype
                H5.H5Dclose(did); // close the Dataset
                continue;
            }
            // Names: We expect the first channel to be "time" and the second to be either "data" or "value"
            if ( !datatypeElements[0].name.toLowerCase().equals("time") || ( !datatypeElements[1].name.toLowerCase().equals("data") && !datatypeElements[1].name.toLowerCase().equals("value") ) ) {
                System.err.println("Dataset " + datasetName + ": didn't have expected channel names TIME and ( DATA or VALUE )");
                H5.H5Tclose(tid); // close the Datatype
                H5.H5Dclose(did); // close the Dataset
                continue;
            }

            //
            // Examine the Dataspace
            //
            long dataspace_id = H5.H5Dget_space(did);
            int rank = H5.H5Sget_simple_extent_ndims(dataspace_id);
            // For now we only support rank==1 (a 1D array)
            if (rank != 1) {
                System.err.println("Dataset " + datasetName + ": rank is not equal to 1");
                H5.H5Sclose(dataspace_id); // close the Dataspace
                H5.H5Tclose(tid); // close the Datatype
                H5.H5Dclose(did); // close the Dataset
                continue;
            }
            long[] dims = new long[rank];
            H5.H5Sget_simple_extent_dims(dataspace_id, dims, null);
            for (int j = 0; j < rank; ++j) {
                System.err.println("\tarray length = " + dims[j]);
            }
            H5.H5Sclose(dataspace_id); // close the Dataspace

            //
            // Write out attributes for this "channel"
            //
            if (bAttributesToFile) {
                // Write attributes to non-standard (non-CT) location
                writeAttributesToFile("CTdata" + File.separator + inFileName + File.separator + topParentGroup + File.separator + "_Attributes" + File.separator + objNames[i] + ".txt", did, "\t");
            } else {
                writeAttributesToCT(attributesCTW, did, datasetName + ".txt", "\t");
            }

            //
            // Extract data
            //
            byte[] read_data = new byte[(int) dims[0] * (int) datatype_len];
            H5.H5Dread(did, tid, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, read_data);
            ByteBuffer bb = null;
            System.err.println("\t" + datatypeElements[0].name + "\t\t" + datatypeElements[1].name);
            for (int j = 0; j < dims[0]; ++j) {
                HDFValue nextVal = new HDFValue();
                nextVal.chanName = objNames[i];
                double nextTime = 0.0;
                for (int k = 0; k < 2; ++k) {
                    bb =  ByteBuffer.wrap(read_data, (int) (j * datatype_len + (int)datatypeElements[k].offset), (int)datatypeElements[k].size).order(ByteOrder.LITTLE_ENDIAN);
                    if (datatypeElements[k].type.equals("double")) {
                        // System.err.print(bb.getDouble());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = bb.getDouble();
                        } else {
                            nextVal.val = new Double(bb.getDouble());
                        }
                    } else if (datatypeElements[k].type.equals("float")) {
                        // System.err.print(bb.getFloat());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getFloat();
                        } else {
                            nextVal.val = new Float(bb.getFloat());
                        }
                    } else if (datatypeElements[k].type.equals("long")) {
                        // System.err.print(bb.getLong());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getLong();
                        } else {
                            nextVal.val = new Long(bb.getLong());
                        }
                    } else if (datatypeElements[k].type.equals("unsigned long")) {
                        // For now, ignore the fact that this is unsigned
                        // System.err.print(bb.getLong());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getLong();
                        } else {
                            nextVal.val = new Long(bb.getLong());
                        }
                    } else if (datatypeElements[k].type.equals("int")) {
                        // System.err.print(bb.getInt());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getInt();
                        } else {
                            nextVal.val = new Integer(bb.getInt());
                        }
                    } else if (datatypeElements[k].type.equals("unsigned int")) {
                        // For now, ignore the fact that this is unsigned
                        // System.err.print(bb.getInt());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getInt();
                        } else {
                            nextVal.val = new Integer(bb.getInt());
                        }
                    } else if (datatypeElements[k].type.equals("short")) {
                        // System.err.print(bb.getShort());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getShort();
                        } else {
                            nextVal.val = new Short(bb.getShort());
                        }
                    } else if (datatypeElements[k].type.equals("unsigned short")) {
                        // For now, ignore the fact that this is unsigned
                        // System.err.print(bb.getShort());
                        if (datatypeElements[k].name.toLowerCase().equals("time")) {
                            nextTime = (double)bb.getShort();
                        } else {
                            nextVal.val = new Short(bb.getShort());
                        }
                    }
                }
                // Add the new data point to the TreeMultimap
                dataMap.put(nextTime,nextVal);
            }
            H5.H5Tclose(tid); // close the Datatype
            H5.H5Dclose(did); // close the Dataset
        }

        // If we were writing attributes to CT, it's time to close the CTwriter
        if (attributesCTW != null) {
            attributesCTW.close();
        }

        //
        // Write out all data (in time then channel order)
        //
        String ctw_destination_folder = "CTdata/" + inFileName + rootGroup + topParentGroup;
        CTwriter ctw = new CTwriter(ctw_destination_folder);
        ctw.setGZipMode(bGzip);
        ctw.setBlockMode(bPack,bZip);
        ctw.setHiResTime(bHiResTime);
        if (flushInterval > 0.0) {
            ctw.autoFlush(flushInterval);
        }
        if (encryptionPW != null) {
            ctw.setPassword(encryptionPW);
        }
        double prevHDFtime = -1.0;
        // NOTE: dataMap.keys() contains all the duplicate time values (as many instances of the same time value as is used across all channels)
        for (double nextTime : dataMap.keys()) {
            if (nextTime < 0) {
                // Don't allow negative timestamps
                System.err.println("Skipping negative HDF5 timestamp " + nextTime);
                continue;
            }
            if (nextTime == prevHDFtime) {
                // skip duplicate time - we've already handled it
                continue;
            }
            prevHDFtime = nextTime;
            double ctTime = baseTime + nextTime;
            ctw.setTime(ctTime);
            // Put all data that is associated with this time (can be multiple points)
            for (HDFValue nextVal : dataMap.get(nextTime)) {
                nextVal.putData(ctw);
            }
        }
        ctw.close();

        //System.err.println("HDF5Constants.H5T_IEEE_F32BE = " + HDF5Constants.H5T_IEEE_F32BE);
        //System.err.println("HDF5Constants.H5T_IEEE_F32LE = " + HDF5Constants.H5T_IEEE_F32LE);
        //System.err.println("HDF5Constants.H5T_IEEE_F64BE = " + HDF5Constants.H5T_IEEE_F64BE);
        //System.err.println("HDF5Constants.H5T_IEEE_F64LE = " + HDF5Constants.H5T_IEEE_F64LE);

        H5.H5Gclose(gid);  // close the Group
        H5.H5Fclose(fid);  // close the File
    }

    /**
     * Write all the attributes (in JSON format) for the given HDF5 object out to CT.
     */
    private void writeAttributesToCT(CTwriter ctw, long objID, String ctChannelName, String printPrefix) throws Exception {
        String attributesStr = getAttributes(objID, printPrefix);
        ctw.putData(ctChannelName,attributesStr);
    }

    /**
     * Write all the attributes (in JSON format) for the given HDF5 object out to the specified file.
     */
    private void writeAttributesToFile(String filename, long objID, String printPrefix) throws Exception {
        // Make sure the given file does not already exist
        File tmpFile = new File(filename);
        if (tmpFile.exists()) {
            throw new Exception("The given Attributes output file already exists: " + filename);
        }
        String attributesStr = getAttributes(objID, printPrefix);
        // Make sure the folders in this path exist
        int finalSlashIdx = filename.lastIndexOf(File.separatorChar);
        String folderPath = filename.substring(0,finalSlashIdx);
        File folderPathF = new File(folderPath);
        folderPathF.mkdirs();
        // Write string to the file
        PrintWriter pw = new PrintWriter(filename);
        pw.print(attributesStr);
        pw.close();
    }

    /**
     * Fetch all attributes associated with the given HDF5 object.
     *
     * We use open source classes taken from HDFView to read the attributes (see hdf/object/h5/H5File).
     *
     * For details on how to read attributes: HDFView source code, src/hdf/object/h5/H5File.java, method getAttribute
     * Here's basic code on reading an attribute:
     * H5O_info_t obj_info = H5.H5Oget_info(gid);
     * if (obj_info.num_attrs > 0) {
     *     for (int i = 0; i < (int) obj_info.num_attrs; i++) {
     *         long aid = H5.H5Aopen_by_idx(gid, ".", HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC, i,
     *         HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
     *         long sid = H5.H5Aget_space(aid);
     *         String attribute_name = H5.H5Aget_name(aid);
     *         System.err.println("/ attribute = " + attribute_name);
     *     }
     * }
     * @param objID          the HDF5 object whose Attributes we want to fetch
     * @param printPrefix    user-friendly prefix to display before the JSON string when printing to stderr
     * @return               a JSON string containing all attributes
     */
    private String getAttributes(long objID, String printPrefix) throws Exception {
        JsonBuilderFactory factory = Json.createBuilderFactory(null);
        JsonArrayBuilder jsonArrayBuilder = factory.createArrayBuilder();
        List<Attribute> objAttributes = H5File.getAttribute(objID);
        for (Attribute nextAtt : objAttributes) {
            // Only handle attributes that are single values
            // if ( (nextAtt.getRank() != 1) || (nextAtt.getDataDims()[0] != 1) ) {
            //     continue;
            // }
            String nameStr = nextAtt.getName();
            String valueStr = nextAtt.toString(",");
            String typeStr = "";
            switch (nextAtt.getType().getDatatypeClass()) {
                case Datatype.CLASS_INTEGER:
                    typeStr = "INTEGER";
                    break;
                case Datatype.CLASS_FLOAT:
                    typeStr = "FLOAT";
                    break;
                case Datatype.CLASS_CHAR:
                    typeStr = "CHAR";
                    break;
                case Datatype.CLASS_STRING:
                    typeStr = "STRING";
                    break;
                case Datatype.CLASS_BITFIELD:
                    typeStr = "BITFIELD";
                    break;
                case Datatype.CLASS_OPAQUE:
                    typeStr = "OPAQUE";
                    break;
                case Datatype.CLASS_COMPOUND:
                    typeStr = "COMPOUND";
                    break;
                case Datatype.CLASS_REFERENCE:
                    typeStr = "REFERENCE";
                    break;
                case Datatype.CLASS_ENUM:
                    typeStr = "ENUM";
                    break;
                case Datatype.CLASS_VLEN:
                    typeStr = "VLEN";
                    break;
                case Datatype.CLASS_ARRAY:
                    typeStr = "ARRAY";
                    break;
                case Datatype.CLASS_TIME:
                    typeStr = "TIME";
                    break;
                default:
                    typeStr = "UNKNOWN";
                    break;
            }
            jsonArrayBuilder.add(factory.createObjectBuilder()
                    .add("name", nameStr)
                    .add("value", valueStr)
                    .add("type", typeStr));
        }
        String attributesStr = jsonArrayBuilder.build().toString();
        System.err.println(printPrefix + attributesStr);
        return attributesStr;
    }

    /**
     * A private class to store data about an element in a compound Datatype
     */
    private class DatatypeElementSpecification {
        public String name = null;
        public String type = null;
        public long size = -1;
        public long offset = -1;

        public DatatypeElementSpecification() {
            // nothing to do
        }

        public String toString() {
            return new String(type + " " + name + ": size = " + size + ", offset = " + offset);
        }
    }

    /**
     * Private class for storing data in the TreeMultimap; each instance of this class stores one data value
     * for one channel.
     */
    private class HDFValue implements Comparable<HDFValue> {
        public String chanName = "";
        // Data will either be stored in val or valStr (not both)
        public Number val;
        public String valStr = null;

        @Override
        public int compareTo(HDFValue otherHDFValue) {
            return chanName.compareTo(otherHDFValue.chanName);
        }

        // Write data to CT using the given CTwriter.
        public void putData(CTwriter ctw) throws Exception {
            if (valStr != null) {
                ctw.putData(chanName + ".txt", valStr);
            } else {
                if (val instanceof Double) {
                    ctw.putData(chanName + ".f64", ((Double) val).doubleValue());
                } else if (val instanceof Float) {
                    ctw.putData(chanName + ".f32", ((Float) val).floatValue());
                } else if (val instanceof Long) {
                    ctw.putData(chanName + ".i64", ((Long) val).longValue());
                } else if (val instanceof Integer) {
                    ctw.putData(chanName + ".i32", ((Integer) val).intValue());
                } else if (val instanceof Short) {
                    ctw.putData(chanName + ".i16", ((Short) val).shortValue());
                }
            }
        }
    }

}
