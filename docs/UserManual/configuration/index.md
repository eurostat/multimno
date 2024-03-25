# Configuration

The multimno application requires from multiple configuration files.  

- **One general configuration file** describing general parameters like file paths, logging, spark and 
common values for all components in the pipeline.  
- **A configuration file per each component** in the pipeline with configuration parameters exclusive of the component. Values defined in these files can override values defined in the general configuration file.