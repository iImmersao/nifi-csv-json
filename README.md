# nifi-csv-json

This project is a simple template for creating a custom processor in Apache NiFi. The two key factors in developing such processors are:

1. Splitting the project into two modules: one for the actual source of the processor and one for the creation of the deployable .nar file.
2. In the NAR module, there must be a file in the resources/META-INF/services directory called org.apache.nifi.processors.Processor, which contains the fully qualified path of your custom processor.

The .nar file created by the Maven build can then be dropped into the NIFI_HOME/extensions directory, where it will be picked up, when NiFi starts. There may still be warnings in the NiFi log that the .nar will not be loaded, but this appears to be misleading, as the processor does become available for use and works as expected.
